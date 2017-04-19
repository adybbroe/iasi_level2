#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016, 2017 Adam.Dybbroe

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

"""
"""

PLATFORMS = {'M01': 'Metop-B',
             'M02': 'Metop-A',
             'M03': 'Metop-C',
             'METOPA': 'Metop-A',
             'METOPB': 'Metop-B',
             'METOPC': 'Metop-C'}

from pyorbital import orbital
from pyresample.spherical_geometry import point_inside, Coordinate


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
