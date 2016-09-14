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

"""A handler for the EUMETSAT IASI level-2 data. Reads hdf5 file and derives
some quantities and outputs a netCDF file.

"""
import os
import h5py
import numpy as np
from netCDF4 import Dataset
from datetime import datetime, timedelta
from trollsift import parser

from .utils import PLATFORMS

NC_COMPRESS_LEVEL = 6

#EPSILON = 0.1
#NODATA = 3.4028235E38 - EPSILON
DATA_UPPER_LIMIT = 1000000
NODATA = -9.

# Example filename:
# IASI_PW3_02_M01_20160418132052Z_20160418132356Z_N_O_20160418140305Z.h5
iasi_file_pattern = "IASI_PW3_02_{platform_name:s}_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{creation_time:%Y%m%d%H%M%S}Z"

VAR_NAMES_AND_TYPES = {
    "temp": ('air_temperature_ml', 'f'),
    "tdew": ('dew_point_temperature', 'f'),
    "qspec": ('specific_humidity_ml', 'f'),
    "pres": ('air_pressure', 'f'),
    "ozone": ('mass_fraction_of_ozone_in_air', 'f'),
}


SURFACE_VAR_NAMES_AND_TYPES = {
    "skin_temp": ('surface_temperature', 'f'),
    "topo": ('surface_elevation', 'f'),
}


# <attribut suffix in input structure>:  (<attribute name in file>)
ATTRIBUTE_NAMES = {
    "standardname": ('standard_name'),
    "longname": ('long_name'),
    "units": ('units'),
    "validrange": ('valid_range'),
    "gain": ('scale_factor'),
    "intercept": ('add_offset'),
}


def qair2rh(qair, temp, press=1013.25):
    """Converting specific humidity into relative humidity:

    NCEP surface flux data does not have RH
    from Bolton 1980 The computation of Equivalent Potential Temperature
    \url{http://www.eol.ucar.edu/projects/ceop/dm/documents/refdata_report/eqns.html}

    @title qair2rh
    @param qair specific humidity, dimensionless (e.g. kg/kg) ratio of water
    mass / total air mass
    @param temp degrees C
    @param press pressure in mb
    @return rh relative humidity, ratio of actual water mixing ratio to
    saturation mixing ratio

    @author David LeBauer

    """

    es = 6.112 * np.exp((17.67 * temp) / (temp + 243.5))
    e = qair * press / (0.378 * qair + 0.622)
    rh = e / es
    rh[rh > 1] = 1
    rh[rh < 0] = 0

    return rh * 100.0  # RHEL in percent!


def rhel2tdew(temp, rhel):
    """Get the dew point temperature from the relative humidity and
    temperature:

    es = 6.112 * exp((17.67 * T)/(T + 243.5));
    e = es * (RH/100.0);
    Td = log(e/6.112)*243.5/(17.67-log(e/6.112));
     where:
       T = temperature in deg C;
       es = saturation vapor pressure in mb;
       e = vapor pressure in mb;
       RH = Relative Humidity in percent;
       Td = dew point in deg C 

    """

    es = 6.112 * np.exp((17.67 * temp) / (temp + 243.5))
    e = es * (rhel / 100.0)
    return np.log(e / 6.112) * 243.5 / (17.67 - np.log(e / 6.112))


def qair2tdew(q__, t__, p__):
    """Get dew point temperature from specific humidity"""

    rhel = qair2rh(q__, t__, p__)
    return rhel2tdew(t__, rhel)


def pressure2height():
    # FIXME!
    pass


class geophys_parameter(object):

    """Container for the geophysical parameter"""

    def __init__(self, data, unit, longname, standardname):
        self.data = data
        self.units = unit
        self.longname = longname
        self.standardname = standardname
        #self.validrange = None


class iasilvl2(object):

    """Extracting the IASI level-2 information from hdf5 files"""

    def __init__(self, filename):
        self.latitudes = None
        self.longitudes = None
        if filename.endswith('.h5'):
            prefix = filename.strip('.h5')
            self.h5_filename = filename
            self.nc_filename = os.path.basename(filename).replace('.h5', '.nc')
        else:
            self.h5_filename = None
            self.nc_filename = filename
            prefix = filename.strip('.nc')

        self.temp = None
        self.skin_temp = None
        self.topo = None
        self.shape_2d = (0, 0)
        self.time_shape = (0,)
        self.shape = (0, 0, 0, 0)
        self.tdew = None
        self.qspec = None
        self.pres = None
        self.ozone = None

        # Location of profile as a string e.g. as in 'N1578;W04600'
        self.locations = None
        piasi = parser.Parser(iasi_file_pattern)
        items = piasi.parse(os.path.basename(prefix))
        self.start_time = items['start_time']
        self.end_time = items['end_time']

        self.platform_name = PLATFORMS.get(
            items['platform_name'], items['platform_name'])
        self.time_origo = datetime(2000, 1, 1)

        if self.h5_filename:
            self._load()
        else:
            self._loadnc()

    def _loadnc(self):

        rootgrp = Dataset(self.nc_filename, 'r')
        self.pres = geophys_parameter(rootgrp.variables['air_pressure'][0, :],
                                      rootgrp.variables['air_pressure'].units,
                                      rootgrp.variables[
                                          'air_pressure'].long_name,
                                      rootgrp.variables['air_pressure'].standard_name)
        self.temp = geophys_parameter(rootgrp.variables['air_temperature_ml'][0, :],
                                      rootgrp.variables[
                                          'air_temperature_ml'].units,
                                      rootgrp.variables[
                                          'air_temperature_ml'].long_name,
                                      rootgrp.variables['air_temperature_ml'].standard_name)
        self.tdew = geophys_parameter(
            rootgrp.variables['dew_point_temperature'][0, :],
            rootgrp.variables['dew_point_temperature'].units,
            rootgrp.variables['dew_point_temperature'].long_name,
            rootgrp.variables['dew_point_temperature'].standard_name)
        self.qspec = geophys_parameter(
            rootgrp.variables['specific_humidity_ml'][0, :],
            rootgrp.variables['specific_humidity_ml'].units,
            rootgrp.variables['specific_humidity_ml'].long_name,
            rootgrp.variables['specific_humidity_ml'].standard_name)
        self.ozone = geophys_parameter(
            rootgrp.variables['mass_fraction_of_ozone_in_air'][0, :],
            rootgrp.variables['mass_fraction_of_ozone_in_air'].units,
            rootgrp.variables['mass_fraction_of_ozone_in_air'].long_name,
            rootgrp.variables['mass_fraction_of_ozone_in_air'].standard_name)

        locs = rootgrp.variables['vcross_name'][:]
        locations = []
        for idx in range(locs.shape[0]):
            locations.append(locs[idx].tostring().strip('\x00'))

        self.locations = np.array(locations)
        self.latitudes = rootgrp.variables['latitude'][:]
        self.longitudes = rootgrp.variables['longitude'][:]

        rootgrp.close()

    def _load(self):
        """Load the original EUMETSAT hdf5 data"""

        with h5py.File(self.h5_filename, 'r') as h5f:

            data = h5f['PWLR/T'][:]
            oshape = data.shape

            # The layout of the IASI lvl2 data is kind of special:
            # I am splitting the 4 FOV dewlls and treat them as four independent
            # pixels. So, one 'scanline' is thus actually only considering two of the
            # four FOV's in each dwell.
            #index1 = np.arange(3, 120, 4)
            #index2 = np.arange(0, 120, 4)
            index1 = np.arange(3, oshape[1] * oshape[0], 4)
            index2 = np.arange(0, oshape[1] * oshape[0], 4)
            index3 = np.arange(2, oshape[1] * oshape[0], 4)
            index4 = np.arange(1, oshape[1] * oshape[0], 4)
            idx_a = np.empty((index1.size + index2.size,),
                             dtype=index1.dtype)
            idx_b = np.empty((index3.size + index4.size,),
                             dtype=index3.dtype)
            idx_a[0::2] = index1
            idx_a[1::2] = index2
            idx_b[0::2] = index3
            idx_b[1::2] = index4
            idx = np.empty((index1.size + index2.size + index3.size + index4.size,),
                           dtype=index1.dtype)
            idx = idx.reshape((oshape[0] * 2, oshape[1] / 2))
            idx_a = idx_a.reshape((oshape[0], oshape[1] / 2))
            idx_b = idx_b.reshape((oshape[0], oshape[1] / 2))
            idx[0::2, :] = idx_a
            idx[1::2, :] = idx_b
            idx = idx.ravel()

            self.latitudes = h5f['L1C/Latitude'][:].ravel()[idx]
            self.latitudes = self.latitudes[np.newaxis, :]
            self.longitudes = h5f['L1C/Longitude'][:].ravel()[idx]
            self.longitudes = self.longitudes[np.newaxis, :]

            data = h5f['PWLR/T'][:]
            oshape = data.shape
            data = data.reshape(oshape[0] * oshape[1], oshape[2]).transpose()
            data = data[:, idx]
            data = data[:, np.newaxis, :]
            self.shape = data.shape
            self.temp = geophys_parameter(data,
                                          'K',
                                          'Air temperature',
                                          'air_temperature_ml')

            data = h5f['PWLR/P'][:]
            data = data.reshape(oshape[0] * oshape[1], oshape[2]).transpose()
            data = data[:, idx]
            data = data[:, np.newaxis, :]
            pressure = np.ma.masked_greater(data,
                                            DATA_UPPER_LIMIT)
            pressure = pressure * 100
            pressure.fillvalue = NODATA
            self.pres = geophys_parameter(pressure,
                                          'Pa',
                                          'Air Pressure',
                                          'air_pressure')
            # Water vapour mixing ratio:
            data = h5f['PWLR/W'][:]
            data = data.reshape(oshape[0] * oshape[1], oshape[2]).transpose()
            data = data[:, idx]
            data = data[:, np.newaxis, :]
            wvmix = np.ma.masked_greater(data, DATA_UPPER_LIMIT)
            self.tdew = geophys_parameter(qair2tdew(
                wvmix, self.temp.data - 273.15, pressure / 100.) + 273.15,
                'K',
                'Dew Point Temperature',
                'dew_point_temperature')
            self.qspec = geophys_parameter(wvmix,
                                           #'kg/kg',
                                           '1',
                                           'Specific Humidity',
                                           'specific_humidity_ml')

            data = h5f['PWLR/O'][:]
            data = data.reshape(oshape[0] * oshape[1], oshape[2]).transpose()
            data = data[:, idx]
            data = data[:, np.newaxis, :]
            ozone = np.ma.masked_greater(data, DATA_UPPER_LIMIT)
            ozone.fillvalue = NODATA
            self.ozone = geophys_parameter(ozone,
                                           '1',
                                           'Ozone mixing ratio vertical profile',
                                           'fraction_of_ozone_in_air')

            # Surface 2d variables:
            #data = h5f['PWLR/Ts'][:].ravel()
            data = h5f['PWLR/Ts'][:].ravel()[idx]
            tskin = np.ma.masked_greater(
                data[np.newaxis, np.newaxis, :], DATA_UPPER_LIMIT)
            tskin.fillvalue = NODATA
            self.skin_temp = geophys_parameter(tskin,
                                               'K',
                                               'Surface skin temperature',
                                               'surface_temperature')
            data = h5f['Maps/Height'][:].ravel()[idx]
            topo = np.ma.masked_greater(
                data[np.newaxis, np.newaxis, :], DATA_UPPER_LIMIT)
            topo.fillvalue = NODATA
            self.topo = geophys_parameter(topo,
                                          'm',
                                          'Topography',
                                          'surface_elevation')

            #stime_day = h5f['L1C']['SensingTime_day'][:]
            stime_day = h5f['L1C']['SensingTime_day'][:][0]
            # msec = h5f['L1C']['SensingTime_msec'][:]
            # dtobj_arr = np.array([(self.time_origo +
            #                        timedelta(days=int(stime_day[idx])) +
            #                        timedelta(microseconds=int(msec[idx])))
            #                       for idx in range(stime_day.shape[0])])
            #self.start_time = dtobj_arr.min()
            #self.end_time = dtobj_arr.max()
            self.shape_2d = self.latitudes.shape
            # self.time_shape = stime_day.shape
            self.time_shape = (1,)

    def ncwrite(self, filename=None):
        """Write the data to a netCDF file"""

        if filename:
            self.nc_filename = filename
        root = Dataset(self.nc_filename, 'w', format='NETCDF3_CLASSIC')

        # Add time as a dimension
        new_shape = (
            1, self.shape[0], self.shape[1], self.shape[2])
        shape = new_shape

        # Add time as a dimension
        # 'time' represents the time (one time for all of the scene)
        root.createDimension('time', shape[0])
        root.createDimension('sigma', 1)
        root.createDimension('l', shape[1])
        root.createDimension('y', shape[2])
        root.createDimension('x', shape[3])
        root.createDimension('height0', 1)
        # 'nv' is used for bounds
        root.createDimension('nv', 2)
        # For place naming
        root.createDimension('nvcross_strlen', 80)
        root.createDimension('nvcross', shape[3] * shape[2] / 60)
        root.createDimension('two', 2)

        create_time_coordinate(root, self.start_time, self.end_time, shape[0])

        # Don't yet know what the nodata value is for lats&lons
        # FIXME!
        create_latlon_var(root, self.latitudes, self.longitudes, -999)

        # Find and write one variable at the time
        for key in vars(self).keys():
            if key in VAR_NAMES_AND_TYPES.keys():
                fillval = NODATA
                var = root.createVariable(VAR_NAMES_AND_TYPES[key][0],
                                          VAR_NAMES_AND_TYPES[key][1],
                                          ('time', 'l', 'y', 'x'),
                                          fill_value=fillval,
                                          zlib=True, complevel=NC_COMPRESS_LEVEL)
                var[:] = getattr(self, key).data

                # Add attributes to the variable
                setattr(var, "coordinates", "longitude latitude")
                for subkey in vars(getattr(self, key)).keys():
                    if subkey in ATTRIBUTE_NAMES.keys():
                        setattr(var, ATTRIBUTE_NAMES[subkey],
                                getattr(getattr(self, key), subkey))

            elif key in SURFACE_VAR_NAMES_AND_TYPES.keys():
                fillval = NODATA
                var = root.createVariable(SURFACE_VAR_NAMES_AND_TYPES[key][0],
                                          SURFACE_VAR_NAMES_AND_TYPES[key][1],
                                          ('time', 'height0', 'y', 'x'),
                                          fill_value=fillval,
                                          zlib=True, complevel=NC_COMPRESS_LEVEL)
                var[:] = getattr(self, key).data

                # Add attributes to the variable
                setattr(var, "coordinates", "longitude latitude")
                for subkey in vars(getattr(self, key)).keys():
                    if subkey in ATTRIBUTE_NAMES.keys():
                        setattr(var, ATTRIBUTE_NAMES[subkey],
                                getattr(getattr(self, key), subkey))

        var = root.createVariable('l',
                                  'f',
                                  ('l'),
                                  fill_value=0,
                                  zlib=True, complevel=NC_COMPRESS_LEVEL)
        var[:] = np.arange(1, shape[1] + 1)
        setattr(var, "long_name", "atmosphere_sigma_coordinate")
        setattr(var, "standard_name", "atmosphere_sigma_coordinate")
        setattr(var, "positive", "up")
        # var = root.createVariable('height',
        #                           'f',
        #                           ('height'),
        #                           fill_value=0,
        #                           zlib=True, complevel=NC_COMPRESS_LEVEL)
        # var[:] = np.arange(1, shape[1] + 1)
        # setattr(var, "long_name", "Height coordinate level above surface")
        # setattr(var, "standard_name", "height_level")
        # setattr(var, "positive", "up")

        var = root.createVariable('sigma', 'f4',  ('sigma'),
                                  fill_value=-1,
                                  zlib=True, complevel=NC_COMPRESS_LEVEL)
        var[:] = 999
        setattr(var, "long_name", "atmosphere_sigma_coordinate")
        setattr(var, "standard_name", "atmosphere_sigma_coordinate")
        setattr(var, "positive", "down")

        nyvar = root.createVariable('y', 'f', ('y',),
                                    fill_value=-1,
                                    zlib=True, complevel=NC_COMPRESS_LEVEL)
        nyvar[:] = np.arange(shape[2])
        setattr(nyvar, "units", "1")
        setattr(nyvar, "standard_name", "projection_y_coordinate")
        setattr(nyvar, "long_name", "y coordinate of projection")

        nxvar = root.createVariable('x', 'f', ('x',),
                                    fill_value=-1,
                                    zlib=True, complevel=NC_COMPRESS_LEVEL)
        nxvar[:] = np.arange(shape[3])
        setattr(nxvar, "units", "1")
        setattr(nxvar, "standard_name", "projection_x_coordinate")
        setattr(nxvar, "long_name", "x coordinate of projection")

        vcrossnamevar = root.createVariable('vcross_name', 'c',
                                            ('nvcross', 'nvcross_strlen'),
                                            zlib=True, complevel=NC_COMPRESS_LEVEL)
        setattr(vcrossnamevar, "bounds", "vcross_bnds")
        # Example:  "N7330;E00500",
        locnames = self.make_position_names()
        #locnames = ['one scanline']
        #vcrossnamevar[0, 0:len("N7330;E00500")] = "N7330;E00500"
        idx = 0
        for start_name, end_name in zip(np.array(locnames)[0:2760:60],
                                        np.array(locnames)[59:2760:60]):
            lname = start_name + ' ' + end_name
            vcrossnamevar[idx, 0:len(lname)] = lname
            idx = idx + 1
        vcrossboundvar = root.createVariable('vcross_bnds', 'i4', ('nvcross', 'two'),
                                             zlib=True, complevel=NC_COMPRESS_LEVEL)
        setattr(vcrossboundvar, "description",
                "Start- and end-position (included) in lat- and lon-dimensions" +
                " for each IASI profile")
        vcrossboundvar[:, 0] = np.arange(0, shape[3], 60)
        vcrossboundvar[:, 1] = np.arange(59, shape[3], 60)

        self._set_global_attributes(root)
        root.close()

    def _set_global_attributes(self, root):
        """Write the global attributes to the netcdf file"""

        # Set attributes that differs from those found in the hdf-file
        setattr(root, "id", self.nc_filename)

        # Set attributes that are not found in 'header'
        setattr(root, "platform", self.platform_name)
        setattr(root, "Conventions", "CF-1.6")
        setattr(
            root, "institution", "Swedish Meteorological and Hydrological Institute")

    def make_position_names(self):
        """From the longitude latitude positions make location names in the form of:
        N7330;E00500

        """

        retv = []
        for lon, lat in zip(self.longitudes.ravel(), self.latitudes.ravel()):
            if lat > 0:
                latname = 'N%.4d' % int(lat * 100)
            else:
                latname = 'S%.4d' % int(abs(lat * 100))

            if lon > 0:
                lonname = 'E%.5d' % int(lon * 100)
            else:
                lonname = 'W%.5d' % int(abs(lon * 100))

            retv.append('%s;%s' % (latname, lonname))

        return retv


def create_latlon_var(root, latitude, longitude, nodata):
    """ Create latitude and longitude variables"""

    lat = root.createVariable('latitude', 'f4',  ('y', 'x'),
                              fill_value=nodata,
                              zlib=True, complevel=NC_COMPRESS_LEVEL)
    lon = root.createVariable('longitude', 'f4',  ('y', 'x'),
                              fill_value=nodata,
                              zlib=True, complevel=NC_COMPRESS_LEVEL)
    lat[:] = latitude
    lon[:] = longitude

    # Add attributes to lat/lon
    setattr(lat, "_CoordinateAxisType", "Lat")
    setattr(lat, "standard_name", "latitude")
    setattr(lat, "long_name", "Latitude at the centre of each pixel")
    setattr(lat, "units", "degrees_north")
    setattr(lat, "valid_range", (-90.0, 90.0))
    setattr(lon, "_CoordinateAxisType", "Lon")
    setattr(lon, "standard_name", "longitude")
    setattr(lon, "long_name", "Longitude at the center of each pixel")
    setattr(lon, "units", "degrees_east")
    setattr(lon, "valid_range", (-180.0, 180.0))

    return 0


def create_time_coordinate(root, start_time, end_time, timesize=1):
    """Create the time coordinate"""

    # Find the time bounds and the middle time
    dtobj_1970 = datetime(1970, 1, 1)

    start_sec = (start_time - dtobj_1970).days * 24 * \
        3600 + (start_time - dtobj_1970).seconds
    end_sec = (end_time - dtobj_1970).days * 24 * \
        3600 + (end_time - dtobj_1970).seconds

    mid_sec = (end_sec + start_sec) / 2
    time_origo = dtobj_1970.strftime('%Y-%m-%d %H:%M:%S.%f')[0:-4]

    # Create the variable and set attributes
    timevar = root.createVariable('time', 'f4', ('time',),
                                  zlib=True, complevel=NC_COMPRESS_LEVEL)
    timevar[:] = np.zeros(timesize, dtype=np.float)
    timevar[0] = mid_sec
    setattr(timevar, "long_name", "time")
    setattr(timevar, "units", "seconds since %s +00:00" % (time_origo))
    setattr(timevar, "bounds", "time_bnds")
    timeboundvar = root.createVariable('time_bnds', 'f4', ('time', 'nv'),
                                       zlib=True, complevel=NC_COMPRESS_LEVEL)
    timeboundvar[0, :] = np.zeros(2, dtype=np.float)
    timeboundvar[0, 0] = start_sec
    timeboundvar[0, 1] = end_sec

    return 0


if __name__ == "__main__":
    TESTFILE = "/home/a000680/data/iasi/IASI_PW3_02_M01_20160530200854Z_20160530201158Z_N_O_20160530203738Z.h5"
    l2p = iasilvl2(TESTFILE)
    l2p.ncwrite()
    that = iasilvl2(l2p.nc_filename)
