#!/usr/bin/env python3
"""Script to fetch IASI level 2 profiles from ftp at EUMETSAT."""
import argparse
import logging
import os
import socket
import tempfile
from datetime import datetime, timedelta
from ftplib import FTP

import numpy as np
from trollsift import Parser

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='ftp retrieval of EARS IASI lvl2 data')
    parser.add_argument("-d", "--dir",
                        help="Destination directory path")
    parser.add_argument("-c", "--cfgdir",
                        help="Config directory")
    parser.add_argument("--hours",
                        help="How far back in time in hours to fetch",
                        type=int)

    args = parser.parse_args()
    outpath = args.dir
    hours = args.hours
    CFG_DIR = args.cfgdir

    DIST = os.environ.get("SMHI_DIST", "elin4")
    if not DIST or DIST == "linda4":
        MODE = "offline"
    else:
        MODE = os.environ.get("SMHI_MODE", "offline")

    CONF = RawConfigParser()
    CFG_FILE = os.path.join(CFG_DIR, "iasi_level2_config.cfg")
    LOG.debug("Config file = " + str(CFG_FILE))
    if not os.path.exists(CFG_FILE):
        raise IOError("Config file %s does not exist!" % CFG_FILE)

    CONF.read(CFG_FILE)
    OPTIONS = {}
    for option, value in CONF.items("DEFAULT"):
        OPTIONS[option] = value

    for option, value in CONF.items(MODE):
        OPTIONS[option] = value

    HOST = OPTIONS["ftp_host"]
    REMOTE_DIRS = [
        os.path.join(OPTIONS["remote_dir"], "PW3_M01"),
        os.path.join(OPTIONS["remote_dir"], "PW3_M02"),
    ]
    USER = OPTIONS["login_user"]
    PASSWD = OPTIONS["login_passwd"]

    today = datetime.today()
    dtime = timedelta(seconds=3600 * hours)
    starttime = today - dtime

    # IASI_PW3_02_M01_20160309180258Z_20160309180554Z_N_O_20160309184345Z.h5
    pattern = "IASI_PW3_02_{platform_name:3s}_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{creation_time:%Y%m%d%H%M%S}Z.h5"
    p__ = Parser(pattern)

    PREFIXES = [
        "IASI_PW3_",
        "IASI_PW3_",
    ]
    ftp = FTP(HOST)
    print("connecting to %s" % HOST)
    ftp.login(USER, PASSWD)
    tempfile.tempdir = outpath
    for remotedir, prefix in zip(REMOTE_DIRS, PREFIXES):
        remotefiles = ftp.nlst(remotedir)
        fnames = [os.path.basename(f) for f in remotefiles]
        dates_remote = [p__.parse(s)["start_time"] for s in fnames]

        rfarr = np.array(remotefiles)
        drarr = np.array(dates_remote)
        remotefiles = rfarr[drarr > starttime].tolist()
        remotefiles = [
            r
            for r in remotefiles
            if (r.endswith(".h5") and os.path.basename(r).startswith(prefix))
        ]

        localfiles = [
            os.path.join(outpath, os.path.basename(f.strip(".h5"))) for f in remotefiles
        ]

        try:
            for remote_file, local_file in zip(remotefiles, localfiles):
                if not os.path.isfile(local_file + ".h5"):
                    out_filename = local_file + ".h5"
                    tmpfilename = tempfile.mktemp()
                    try:
                        file_handle = open(tmpfilename, "wb")
                        ftp.retrbinary("RETR " + remote_file, file_handle.write)
                        file_handle.close()
                    except socket.error:
                        print("Cannot get file %s" % remote_file)
                        if os.path.exists:
                            os.remove(tmpfilename)
                        continue

                    os.rename(tmpfilename, out_filename)
                    print("Retrieved file %s" % (out_filename))
                else:
                    print("File already saved, skipping: %s.h5" % (local_file))

        except socket.error:
            print("Exiting")
            ftp.quit()
            break

    print("Exiting")
    ftp.quit()
