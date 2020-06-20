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
NUM_PUT_TASK = 10
MEMCACHE_TIMEOUT = 1
MEMCACHE_RETRY = 3
MEMCACHE_RETRY_DELAY = 0.1

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr], socket_timeout=MEMCACHE_TIMEOUT)
            for ind in range(MEMCACHE_RETRY):
                if memc.set(key, packed):
                    break
                time.sleep(ind * MEMCACHE_RETRY_DELAY)
                if ind == 2:
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
        while True:
            self.next_task = self.task_req.get()
            if self.next_task is None:
                if self.processed != 0:
                    self.err_rate = float(self.errors) / self.processed
                if self.err_rate < NORMAL_ERR_RATE:
                    logging.info("{} - Acceptable error rate ({}). Successfull load".format(self.proc_name, self.err_rate))
                else:
                    logging.error("{} - High error rate ({} > {}). Failed load".format(self.proc_name, self.err_rate, NORMAL_ERR_RATE))
                break
            self.appsinstalled = parse_appsinstalled(self.next_task)
            self.memc_addr = self.mem.get(self.appsinstalled.dev_type)
            if not self.memc_addr:
                self.errors += 1
                logging.error("Unknow device type: {}".format(self.appsinstalled.dev_type))
                continue
            self.ok = insert_appsinstalled(self.memc_addr, self.appsinstalled, self.options.dry)
            if not self.ok:
                self.errors += 1
            else:
                self.processed += 1
            
 
class Task(multiprocessing.Process):
    def __init__(self, task_req, path, num_task, num_wr):
        multiprocessing.Process.__init__(self)
        self.task_req = task_req
        self.path = path
        self.num_task = num_task
        self.num_wr = num_wr

    def run(self):
        for file_name in sorted(glob.iglob(self.path), key=os.path.getmtime):
            with gzip.open(file_name) as fn:
                lines = iter(fn) 
                while True:
                    try:
                        for _ in range(self.num_task):
                            line = lines.__next__().strip()
                            if line:
                                self.task_req.put(line)
                    except StopIteration:
                        break
            dot_rename(file_name)
        for _ in range(self.num_wr):
            self.task_req.put(None)

 
def main(options):
    device_memc = {
        b"idfa": options.idfa,
        b"gaid": options.gaid,
        b"adid": options.adid,
        b"dvid": options.dvid,
    }
    tasks = multiprocessing.Queue()
    cpu_num = multiprocessing.cpu_count()
    num_writer = cpu_num - 1 if cpu_num > 2 else 2
    logging.info('Will be create %d processes' % num_writer)
    writers_mem = [WritedInMemcached(tasks, device_memc, options) for i in range(num_writer)]
    
    for w in writers_mem:
        w.start()

    task = Task(tasks, options.pattern , NUM_PUT_TASK, num_writer)
    task.start()
    task.join()
    
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
