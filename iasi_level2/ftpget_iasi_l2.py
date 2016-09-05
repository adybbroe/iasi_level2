#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Script to fetch IASI level 2 profiles from ftp at EUMETSAT. It checks if
the granule is inside our area of interest (euron1) and downloads the data and
converts it to netCDF on the fly.

"""

import socket
from ftplib import FTP
from datetime import datetime, timedelta
import os
import tempfile
import numpy as np
from iasi_lvl2 import iasilvl2, PLATFORMS

from trollsift import Parser

today = datetime.today()
oneday = timedelta(1)
yesterday = today - oneday

HOST = 'ftp.eumetsat.int'
# ftp://ftp.eumetsat.int/pub/EUM/out/RSP/hultberg/PW3_M01/
LOCAL_DIR = '/data/prod/satellit/iasi'
#LOCAL_DIR = '/home/a000680/data/iasi'
#LOCAL_DIR = '/data/temp/AdamD/iasi'

#    p__ = Parser(
#        '{band_id:s}_{satid:s}_d{start_date:%Y%m%d}_t{start_time:%H%M%S%f}_e{end_time:%H%M%S%f}_b{orbit:s}_c{production_time:s}_noaa_ops.h5')
# Take the first file and determine the orbit number from that one:
#    items = p__.parse(files[0])

# IASI_PW3_02_M01_20160309180258Z_20160309180554Z_N_O_20160309184345Z.h5
pattern = 'IASI_PW3_02_{platform_name:3s}_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{creation_time:%Y%m%d%H%M%S}Z.h5'
p__ = Parser(pattern)

from pyorbital import orbital
from pyresample import utils as pr_utils
from pyresample.spherical_geometry import point_inside, Coordinate

from utils import granule_inside_area


for day in [today, yesterday]:
    dayofyear = day.strftime("%j")
    year = day.strftime("%Y")

    IASI_REMOTE_DIR = ['/pub/EUM/out/RSP/hultberg/PW3_M01/',
                       ]
    PREFIXES = ['IASI_PW3_']

    ftp = FTP(HOST)
    print ("connecting to %s" % HOST)
    ftp.login('ftp', 'adam.dybbroe@smhi.se')

    tempfile.tempdir = LOCAL_DIR
    for (remotedir, prefix) in zip(IASI_REMOTE_DIR, PREFIXES):
        remotefiles = ftp.nlst(remotedir)
        fnames = [os.path.basename(f) for f in remotefiles]
        dates_remote = [p__.parse(s)['start_time'] for s in fnames]

        rfarr = np.array(remotefiles)
        drarr = np.array(dates_remote)
        remotefiles = rfarr[drarr > day - oneday].tolist()
        remotefiles = [r for r in remotefiles if (
            r.endswith('.h5') and os.path.basename(r).startswith(prefix))]

        localfiles = [
            os.path.join(LOCAL_DIR, os.path.basename(f.strip('.h5'))) for f in remotefiles]

        area_def = pr_utils.load_area('/home/a000680/usr/src/pytroll-config/etc/areas.def',
                                      areaid)

        try:
            for (remote_file, local_file) in zip(remotefiles, localfiles):
                if not os.path.isfile(local_file + '.nc'):
                    # Check if the granule is inside the area of interest:
                    items = p__.parse(os.path.basename(remote_file))
                    inside = granule_inside_area(items['start_time'],
                                                 items['end_time'],
                                                 items['platform_name'],
                                                 area_def)
                    if not inside:
                        print("Granule %s outside area..." %
                              os.path.basename(remote_file))
                        continue

                    #tmpfilename = tempfile.mktemp() + '.h5'
                    tmpfilename = local_file + '.h5'
                    nctmpfilename = tempfile.mktemp()
                    try:
                        file_handle = open(tmpfilename, 'wb')
                        ftp.retrbinary(
                            'RETR ' + remote_file, file_handle.write)
                        file_handle.close()
                    except socket.error:
                        print("Cannot get file %s" % remote_file)
                        if os.path.exists:
                            os.remove(tmpfilename)
                        continue

                    # File conversion hdf5 -> nc:
                    l2p = iasilvl2(tmpfilename)
                    l2p.ncwrite(nctmpfilename)
                    os.rename(nctmpfilename, local_file + '.nc')
                    # os.remove(tmpfilename)
                    print("Retrieved %s and converted to %s" %
                          (remote_file, local_file + '.nc'))
                else:
                    print("File already saved, skipping: %s.h5" % (local_file))

        except socket.error:
            print "Exiting"
            ftp.quit()
            break

    print "Exiting"
    ftp.quit()
