#!/usr/bin/env python3
"""A posttroll runner that takes ears-iasi level-2 hdf5 files and convert to netCDF."""

import argparse
import logging
import os
import socket
import sys
import tempfile
import threading
from configparser import RawConfigParser
from datetime import datetime, timedelta
from multiprocessing import Manager, Pool
from queue import Empty
from urllib.parse import urlparse
from pathlib import Path

import netifaces
import posttroll.subscriber
from posttroll.message import Message
from posttroll.publisher import Publish
from pyresample import load_area

from .constants import DEFAULT_LOG_FORMAT, DEFAULT_TIME_FORMAT, MODE, PLATFORMS
from .iasi_lvl2 import IasiLvl2
from .utils import convert_to_path, granule_inside_area

parser = argparse.ArgumentParser(
    description="Conversion from ears-iasi level-2 hdf5 data to netCDF."
)
parser.add_argument(
    "-c",
    "--config-file",
    help="Config file",
    default="configs/iasi_level2_config.cfg",
    type=convert_to_path,
)
parser.add_argument(
    "-a",
    "--areas-file",
    help="File containing definition of areas.",
    default="configs/areas.def",
    type=convert_to_path,
)
args = parser.parse_args()

logger = logging.getLogger(__name__)

logger.debug(f"Reading configs from file {args.config_file}")
config = RawConfigParser()
config.read(args.config_file)

OPTIONS = dict(config.items("DEFAULT") + config.items(MODE))
OUTPUT_PATH = OPTIONS["output_path"]


def get_local_ips():
    inet_addrs = [
        netifaces.ifaddresses(iface).get(netifaces.AF_INET)
        for iface in netifaces.interfaces()
    ]
    return [ip["addr"] for addrs in inet_addrs for ip in addrs if addrs is not None]


def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    logger.debug("Release/reset job-key '%s' from job registry", key)
    try:
        objdict.pop(key)
    except KeyError:
        logger.warning(
            "Nothing to reset/release: Register didn't contain any entry matching '%s' ",
            key,
        )


class FilePublisher(threading.Thread):

    """A publisher for the iasi level2 netCDF files. Picks up the return value from
    the format_conversion when ready, and publishes the files via posttroll

    """

    def __init__(self, queue):
        super().__init__()
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):
        with Publish("ears_iasi_lvl2_converter", 0, ["netCDF/3"]) as publisher:
            while self.loop:
                retv = self.queue.get()
                if retv is not None:
                    logger.info("Publish the IASI level-2 netcdf file")
                    publisher.send(retv)


class FileListener(threading.Thread):
    """A file listener class, to listen for incoming messages with a
    relevant file for further processing"""

    def __init__(self, queue):
        super().__init__()
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):
        with posttroll.subscriber.Subscribe(
            "", [OPTIONS["posttroll_topic"]], True
        ) as subscr:
            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                if self.check_message(msg):
                    logger.debug("Relevant msg detected. Put it in the queue.")
                    self.queue.put(msg)

    def check_message(self, msg):
        if not msg:
            return False

        if msg.type != 'file':
            logger.info(f"Message type {str(msg.type)} is not 'file'. Continue...")
            return False

        urlobj = urlparse(msg.data["uri"])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if urlobj.netloc and (url_ip not in get_local_ips()):
            logger.warning(
                "Server %s not the current one: %s", server, socket.gethostname()
            )
            return False

        if "platform_name" not in msg.data or "start_time" not in msg.data:
            logger.warning("Message is lacking crucial fields...")
            return False

        logger.debug("Ok: message = %s", str(msg))
        return True


def create_message(resultfile, mda):
    """Create the posttroll message"""

    servername = OPTIONS.get("servername", socket.gethostname())
    to_send = mda.copy()
    to_send["uri"] = resultfile
    to_send["uid"] = Path(resultfile).name
    # Product:
    if 'vcross' in resultfile:
        to_send["product"] = 'iasi_l2_vcross'
    if 'vprof' in resultfile:
        to_send["product"] = 'iasi_l2_vprof'
    to_send["type"] = "IASI-L2"
    to_send["format"] = "netCDF"
    to_send["data_processing_level"] = "3"
    environment = MODE
    return Message(
        "/"
        + to_send["type"]
        + "/"
        + to_send["data_processing_level"]
        + "/polar/regional/",
        "file",
        to_send,
    ).encode()


def format_conversion(mda, scene, job_id, publish_q):
    """Read the hdf5 file and add parameters and convert to netCDF"""
    try:
        logger.debug("IASI L2 format converter: Start...")

        dummy, fname = os.path.split(scene["filename"])
        tempfile.tempdir = OUTPUT_PATH

        area_def = load_area(args.areas_file, OPTIONS["area_of_interest"])
        logger.debug("Platform name = %s", scene["platform_name"])

        # Check if the granule is inside the area of interest:
        inside = granule_inside_area(
            scene["starttime"],
            scene["endtime"],
            PLATFORMS.get(scene["platform_name"], scene["platform_name"]),
            area_def,
        )

        if not inside:
            logger.info("Data outside area of interest. Ignore...")
            return

        # File conversion hdf5 -> nc:
        logger.info("Read the IASI hdf5 file %s", scene["filename"])
        l2p = IasiLvl2(scene["filename"])
        nctmpfilename = tempfile.mktemp()
        l2p.ncwrite(nctmpfilename)
        logger.debug(f"Filename = {l2p.nc_filename}")
        _tmp_nc_filename = fname.split(".")[0]
        _tmp_nc_filename_r1 = _tmp_nc_filename.replace("+", "_")
        _tmp_nc_filename = _tmp_nc_filename_r1.replace(",", "_")

        local_path_prefix = os.path.join(OUTPUT_PATH, _tmp_nc_filename)
        result_file = f"{local_path_prefix}_vprof.nc"
        logger.info("Rename netCDF file %s to %s", l2p.nc_filename, result_file)
        os.rename(l2p.nc_filename, result_file)

        pubmsg = create_message(result_file, mda)
        logger.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

        nctmpfilename = tempfile.mktemp()
        result_file = f"{local_path_prefix}_vcross.nc"
        l2p.ncwrite(nctmpfilename, vprof=False)
        logger.info("Rename netCDF file %s to %s", l2p.nc_filename, result_file)
        os.rename(l2p.nc_filename, result_file)

        pubmsg = create_message(result_file, mda)
        logger.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

        if isinstance(job_id, datetime):
            dt_ = datetime.utcnow() - job_id
            logger.info(
                "IASI level-2 netCDF file "
                + str(job_id)
                + " finished. It took: "
                + str(dt_)
            )
        else:
            logger.warning("Job entry is not a datetime instance: %s", job_id)

    except:
        logger.exception("Failed in IASI L2 format converter...")
        raise


def iasi_level2_runner():
    """Listens and triggers processing"""

    logger.info("*** Start the extraction and conversion of ears iasi level2 profiles")

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
            logger.debug("Empty listener queue...")
            continue

        logger.debug("Number of threads currently alive: %d", threading.active_count())

        if "start_time" in msg.data:
            start_time = msg.data["start_time"]
        elif "nominal_time" in msg.data:
            start_time = msg.data["nominal_time"]
        else:
            logger.warning("Neither start_time nor nominal_time in message!")
            start_time = None

        if "end_time" in msg.data:
            end_time = msg.data["end_time"]
        else:
            logger.warning("No end_time in message!")
            end_time = start_time + timedelta(seconds=60 * 15) if start_time else None
        if not start_time or not end_time:
            logger.warning("Missing either start_time or end_time or both!")
            logger.warning("Ignore message and continue...")
            continue

        sensor = str(msg.data["sensor"])
        platform_name = msg.data["platform_name"]

        keyname = f"{platform_name}_{start_time.strftime('%Y%m%d%H%M')}"

        jobs_dict[keyname] = datetime.now()

        urlobj = urlparse(msg.data["uri"])
        path, fname = os.path.split(urlobj.path)
        logger.debug("path %s, filename = %s", path, fname)

        scene = {
            "platform_name": platform_name,
            "starttime": start_time,
            "endtime": end_time,
            "sensor": sensor,
            "filename": urlobj.path,
        }

        # if keyname not in jobs_dict:
        #    LOG.warning("Scene-run seems unregistered! Forget it...")
        #    continue
        pool.apply_async(
            format_conversion, (msg.data, scene, jobs_dict[keyname], publisher_q)
        )

        # Block any future run on this scene for x minutes from now
        # x = 5
        thread_job_registry = threading.Timer(
            5 * 60.0, reset_job_registry, args=(jobs_dict, keyname)
        )
        thread_job_registry.start()

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


def main():
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger("").addHandler(handler)
    logging.getLogger("").setLevel(logging.DEBUG)
    logging.getLogger("posttroll").setLevel(logging.INFO)

    iasi_level2_runner()
