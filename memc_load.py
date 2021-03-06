#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
import appsinstalled_pb2
import memcache
import time
import multiprocessing

NORMAL_ERR_RATE = 0.01
NUM_PUT_TASK = 100
MEMCACHE_TIMEOUT = 1
MEMCACHE_RETRY = 3
MEMCACHE_RETRY_DELAY = 0.1

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, packeds, dry_run=False):
    try:
        if dry_run:
            logging.debug("{} -> {}".format(memc_addr, str(packeds).replace("\n", " ")))
        else:
            for attempt in range(MEMCACHE_RETRY):
                if not memc_addr.set_multi(packeds):
                    break
                time.sleep(attempt * MEMCACHE_RETRY_DELAY)
                if attempt == MEMCACHE_RETRY-1:
                    return False
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split(b"\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(b",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(b",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class WritedInMemcached(multiprocessing.Process):
    def __init__(self, task_req, mem, options):
        multiprocessing.Process.__init__(self)
        self.task_req = task_req
        self.mem = mem
        self.options = options
        self.errors = 0
        self.processed = 0
        self.err_rate = 0.0
        
    def run(self):
        self.proc_name = self.name
        logging.info('Will be create %s processes' % self.proc_name)
        while True:
            if self.task_req.empty():
                continue
            self.next_task = self.task_req.get(timeout=0.1)
            if self.next_task is None:
                logging.info('Closed %s processes' % self.proc_name)
                if self.processed != 0:
                    self.err_rate = float(self.errors) / self.processed
                if self.err_rate < NORMAL_ERR_RATE:
                    logging.info("{} - Acceptable error rate ({}). Successfull load".format(self.proc_name, self.err_rate))
                else:
                    logging.error("{} - High error rate ({} > {}). Failed load".format(self.proc_name, self.err_rate, NORMAL_ERR_RATE))
                break
            self.memc_addr = self.mem.get(self.next_task[0])
            if not self.memc_addr:
                self.errors += 1
                logging.error("Unknow device type: {}".format(self.next_task[0]))
                continue
            self.ok = insert_appsinstalled(self.memc_addr, self.next_task[1], self.options.dry)
            if not self.ok:
                self.errors += 1
            else:
                self.processed += 1


def serialised_line(line):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = line.lat
    ua.lon = line.lon
    key = "%s:%s" % (line.dev_type, line.dev_id)
    ua.apps.extend(line.apps)
    packed = ua.SerializeToString()
    return key, packed                


class Task(multiprocessing.Process):
    def __init__(self, task_req, files, num_task, num_wr):
        multiprocessing.Process.__init__(self)
        self.task_req = task_req
        self.files = files
        self.num_task = num_task
        self.num_wr = num_wr

    def run(self):
        self.proc_name = self.name
        logging.info('Will be create %s processes' % self.proc_name)
        while True:
            file_name = self.files.get(timeout=0.1)
            if file_name == None:
                logging.info('Closed %s processes' % self.proc_name)
                break
            with gzip.open(file_name) as fn:
                pack = {}
                for line in fn:
                    line = line.strip()
                    line_pars = parse_appsinstalled(line)
                    key, packed = serialised_line(line_pars)
                    pack_temp = pack.get(line_pars.dev_type)
                    if not pack_temp:
                        pack_temp ={}
                    pack_temp.update({key:packed})
                    pack.update({line_pars.dev_type:pack_temp})
                    for ind_memc in pack.keys():
                        if len(pack[ind_memc]) >= NUM_PUT_TASK:
                            self.task_req.put([ind_memc, pack[ind_memc]])
                            pack[ind_memc] = {}
                else: 
                     for ind_memc in pack.keys():
                            self.task_req.put([ind_memc, pack[ind_memc]])
            dot_rename(file_name)

 
def main(options):
    device_memc = {
        b"idfa": options.idfa,
        b"gaid": options.gaid,
        b"adid": options.adid,
        b"dvid": options.dvid,
    }
    
    qtasks = multiprocessing.Queue()
    qfiles = multiprocessing.Queue() 
    cpu_num = multiprocessing.cpu_count()
    num_tasks = 2
    num_writer = cpu_num - num_tasks if cpu_num > 2 else 2
    
    memc_dev_adr = {device : memcache.Client([device_memc[device]], socket_timeout=MEMCACHE_TIMEOUT) for device in device_memc.keys()}
    writers_mem = [WritedInMemcached(qtasks, memc_dev_adr, options) for i in range(num_writer)]
    
    for w in writers_mem:
        w.start()

    for file_name in sorted(glob.iglob(options.pattern), key=os.path.getmtime):
        qfiles.put(file_name)
    else:
        for _ in range(num_tasks):
            qfiles.put(None)

    task_id = [Task(qtasks, qfiles, NUM_PUT_TASK, num_writer) for i in range(num_tasks)]
    for task in task_id:
        task.start()
    
    for task in task_id:
        task.join()
    
    for _ in range(num_writer):
        qtasks.put(None)
            
    for w in writers_mem:
        w.join()

    
def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="LOG/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)
    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
