#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2017, 2020 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""A posttroll runner that takes ears-iasi level-2 hdf5 files and 
convert to netCDF
"""

import logging
import os
import socket
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from multiprocessing import Manager, Pool

import netifaces
import posttroll.subscriber
from ConfigParser import RawConfigParser
from posttroll.message import Message
from posttroll.publisher import Publish
from pyresample import utils as pr_utils
from Queue import Empty
from urlparse import urlparse

from iasi_level2.iasi_lvl2 import iasilvl2
from iasi_level2.utils import granule_inside_area

LOG = logging.getLogger(__name__)

CFG_DIR = os.environ.get('IASI_LVL2_CONFIG_DIR', './')
DIST = os.environ.get("SMHI_DIST", 'elin4')
if not DIST or DIST == 'linda4':
    MODE = 'offline'
else:
    MODE = os.environ.get("SMHI_MODE", 'offline')

CONF = RawConfigParser()
CFG_FILE = os.path.join(CFG_DIR, "iasi_level2_config.cfg")
LOG.debug("Config file = " + str(CFG_FILE))
AREA_DEF_FILE = os.path.join(CFG_DIR, "areas.def")
if not os.path.exists(CFG_FILE):
    raise IOError('Config file %s does not exist!' % CFG_FILE)

CONF.read(CFG_FILE)

PLATFORMS = {'metopa': 'Metop-A', 'metopb': 'Metop-B'}

OPTIONS = {}
for option, value in CONF.items("DEFAULT"):
    OPTIONS[option] = value

for option, value in CONF.items(MODE):
    OPTIONS[option] = value

OUTPUT_PATH = OPTIONS['output_path']
AREA_ID = OPTIONS['area_of_interest']

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

servername = None
servername = socket.gethostname()
SERVERNAME = OPTIONS.get('servername', servername)


sat_dict = {'npp': 'Suomi NPP',
            'noaa19': 'NOAA 19',
            'noaa18': 'NOAA 18',
            'noaa15': 'NOAA 15',
            'aqua': 'Aqua',
            'terra': 'Terra',
            'metop-b': 'Metop-B',
            'metop-a': 'Metop-A',
            'metop-c': 'Metop-C'
            }


def get_local_ips():
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Register didn't contain any entry matching: " +
                    str(key))
    return


class FilePublisher(threading.Thread):

    """A publisher for the iasi level2 netCDF files. Picks up the return value from
    the format_conversion when ready, and publishes the files via posttroll

    """

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('ears_iasi_lvl2_converter', 0, ['netCDF/3', ]) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the IASI level-2 netcdf file")
                    publisher.send(retv)


class FileListener(threading.Thread):

    """A file listener class, to listen for incoming messages with a 
    relevant file for further processing"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with posttroll.subscriber.Subscribe('', [OPTIONS['posttroll_topic'], ],
                                            True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.debug("Put the message on the queue...")
                    self.queue.put(msg)

    def check_message(self, msg):

        if not msg:
            return False

        urlobj = urlparse(msg.data['uri'])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if urlobj.netloc and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s",
                        str(server),
                        socket.gethostname())
            return False

        if ('platform_name' not in msg.data or
                'start_time' not in msg.data):
            LOG.warning(
                "Message is lacking crucial fields...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True


def create_message(resultfile, mda):
    """Create the posttroll message"""

    to_send = mda.copy()
    to_send['uri'] = ('ssh://%s/%s' % (SERVERNAME, resultfile))
    to_send['uid'] = resultfile
    to_send['type'] = 'netCDF'
    to_send['format'] = 'IASI-L2'
    to_send['data_processing_level'] = '3'
    environment = MODE
    pub_message = Message('/' + to_send['format'] + '/' +
                          to_send['data_processing_level'] +
                          environment +
                          '/polar/regional/',
                          "file", to_send).encode()

    return pub_message


def format_conversion(mda, scene, job_id, publish_q):
    """Read the hdf5 file and add parameters and convert to netCDF

    """
    try:
        LOG.debug("IASI L2 format converter: Start...")

        dummy, fname = os.path.split(scene['filename'])
        tempfile.tempdir = OUTPUT_PATH

        area_def = pr_utils.load_area(AREA_DEF_FILE, AREA_ID)
        LOG.debug("Platform name = %s", scene['platform_name'])

        # Check if the granule is inside the area of interest:
        inside = granule_inside_area(scene['starttime'],
                                     scene['endtime'],
                                     PLATFORMS.get(scene['platform_name'],
                                                   scene['platform_name']),
                                     area_def)

        if not inside:
            LOG.info("Data outside area of interest. Ignore...")
            return

        # File conversion hdf5 -> nc:
        LOG.info("Read the IASI hdf5 file %s", scene['filename'])
        l2p = iasilvl2(scene['filename'])
        nctmpfilename = tempfile.mktemp()
        l2p.ncwrite(nctmpfilename)
        _tmp_nc_filename = fname.split('.')[0]
        _tmp_nc_filename_r1 = _tmp_nc_filename.replace('+', '_')
        _tmp_nc_filename = _tmp_nc_filename_r1.replace(',', '_')

        local_path_prefix = os.path.join(OUTPUT_PATH, _tmp_nc_filename)
        result_file = local_path_prefix + '_vprof.nc'
        LOG.info("Rename netCDF file %s to %s", l2p.nc_filename, result_file)
        os.rename(l2p.nc_filename, result_file)

        nctmpfilename = tempfile.mktemp()
        result_file = local_path_prefix + '_vcross.nc'
        l2p.ncwrite(nctmpfilename, vprof=False)
        LOG.info("Rename netCDF file %s to %s", l2p.nc_filename, result_file)
        os.rename(l2p.nc_filename, result_file)

        pubmsg = create_message(result_file, mda)
        LOG.info("Sending: " + str(pubmsg))
        publish_q.put(pubmsg)

        if isinstance(job_id, datetime):
            dt_ = datetime.utcnow() - job_id
            LOG.info("IASI level-2 netCDF file " + str(job_id) +
                     " finished. It took: " + str(dt_))
        else:
            LOG.warning(
                "Job entry is not a datetime instance: " + str(job_id))

    except:
        LOG.exception('Failed in IASI L2 format converter...')
        raise


def iasi_level2_runner():
    """Listens and triggers processing"""

    LOG.info(
        "*** Start the extraction and conversion of ears iasi level2 profiles")

    pool = Pool(processes=6, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    jobs_dict = {}
    while True:

        try:
            msg = listener_q.get()
        except Empty:
            LOG.debug("Empty listener queue...")
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))

        if 'start_time' in msg.data:
            start_time = msg.data['start_time']
        elif 'nominal_time' in msg.data:
            start_time = msg.data['nominal_time']
        else:
            LOG.warning("Neither start_time nor nominal_time in message!")
            start_time = None

        if 'end_time' in msg.data:
            end_time = msg.data['end_time']
        else:
            LOG.warning("No end_time in message!")
            if start_time:
                end_time = start_time + timedelta(seconds=60 * 15)
            else:
                end_time = None

        if not start_time or not end_time:
            LOG.warning("Missing either start_time or end_time or both!")
            LOG.warning("Ignore message and continue...")
            continue

        sensor = str(msg.data['sensor'])
        platform_name = msg.data['platform_name']

        keyname = (str(platform_name) + '_' +
                   str(start_time.strftime('%Y%m%d%H%M')))

        jobs_dict[keyname] = datetime.utcnow()

        urlobj = urlparse(msg.data['uri'])
        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))

        scene = {'platform_name': platform_name,
                 'starttime': start_time,
                 'endtime': end_time,
                 'sensor': sensor,
                 'filename': urlobj.path}

        # if keyname not in jobs_dict:
        #    LOG.warning("Scene-run seems unregistered! Forget it...")
        #    continue
        pool.apply_async(format_conversion,
                         (msg.data, scene,
                          jobs_dict[
                              keyname],
                          publisher_q))

        # Block any future run on this scene for x minutes from now
        # x = 5
        thread_job_registry = threading.Timer(
            5 * 60.0, reset_job_registry, args=(jobs_dict, keyname))
        thread_job_registry.start()

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


if __name__ == "__main__":

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('ears_iasi_lvl2_converter')
    iasi_level2_runner()
