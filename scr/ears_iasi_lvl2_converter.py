#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Adam.Dybbroe

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

"""A posttroll runner that takes ears-iasi level-2 hdf5 files and convert to netCDF
"""

import os
from ConfigParser import RawConfigParser
import logging
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

import sys
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
import tempfile

sat_dict = {'npp': 'Suomi NPP',
            'noaa19': 'NOAA 19',
            'noaa18': 'NOAA 18',
            'noaa15': 'NOAA 15',
            'aqua': 'Aqua',
            'terra': 'Terra',
            'metop-b': 'Metop-B',
            'metop-a': 'Metop-A',
            }

from iasi_level2.iasi_lvl2 import iasilvl2
from pyresample import utils as pr_utils
from iasi_level2.utils import granule_inside_area


def format_conversion(jobreg, message, **kwargs):
    """Read the hdf5 file and add parameters and convert to netCDF

    """
    LOG.info("")
    LOG.info("job-registry dict: " + str(jobreg))
    LOG.info("\tMessage:")
    LOG.info(message)
    urlobj = urlparse(message.data['uri'])
    dummy, fname = os.path.split(urlobj.path)

    tempfile.tempdir = OUTPUT_PATH

    area_def = pr_utils.load_area(AREA_DEF_FILE, AREA_ID)

    # Check if the granule is inside the area of interest:
    inside = granule_inside_area(message['start_time'],
                                 message['end_time'],
                                 message['platform_name'],
                                 area_def)

    if inside:
        # File conversion hdf5 -> nc:
        l2p = iasilvl2(urlobj.path)
        nctmpfilename = tempfile.mktemp()
        l2p.ncwrite(nctmpfilename)
        local_path_prefix = os.path.join(OUTPUT_PATH, fname.split('.')[0])
        os.rename(nctmpfilename, local_path_prefix + '.nc')
    else:
        LOG.info("Data outside area of interest. Ignore...")

    return jobreg


def iasi_level2_runner():
    """Listens and triggers processing"""

    LOG.info(
        "*** Start the extraction and conversion of ears iasi level2 profiles")
    with posttroll.subscriber.Subscribe('', [OPTIONS['posttroll_topic'], ],
                                        True) as subscr:
        with Publish('ears_iasi_lvl2_converter', 0) as publisher:
            job_registry = {}
            for msg in subscr.recv():
                job_registry = format_conversion(
                    job_registry, msg, publisher=publisher)
                # Cleanup in registry (keep only the last 5):
                keys = job_registry.keys()
                if len(keys) > 5:
                    keys.sort()
                    job_registry.pop(keys[0])

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
