#!/usr/bin/env python3
"""Utility/helper functions and classes."""
from pathlib import Path

from pyorbital import orbital
from pyresample.spherical_geometry import Coordinate, point_inside

from .constants import PLATFORMS


def convert_to_path(fpath, check_existence=True):
    """Convert input to a pathlib.Path object."""
    fpath = Path(fpath)
    if check_existence and not fpath.is_file():
        raise FileNotFoundError(fpath)
    return fpath


def granule_inside_area(start_time, end_time, platform_name, area_def):
    """Check if the IASI granule is over area interest, using the times from the
    filename

    """

    metop = orbital.Orbital(PLATFORMS.get(platform_name, platform_name))
    corners = area_def.corners

    is_inside = False
    for dtobj in [start_time, end_time]:
        lon, lat, dummy = metop.get_lonlatalt(dtobj)
        point = Coordinate(lon, lat)
        if point_inside(point, corners):
            is_inside = True
            break

    return is_inside
