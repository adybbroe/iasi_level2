#!/usr/bin/env python3
"""Constants used throughout the package."""

NC_COMPRESS_LEVEL = 6

# EPSILON = 0.1
# NODATA = 3.4028235E38 - EPSILON
DATA_UPPER_LIMIT = 1000000
NODATA = -9.0

# Example filename:
# IASI_PW3_02_M01_20160418132052Z_20160418132356Z_N_O_20160418140305Z.h5
# iasi_file_pattern = "IASI_PW3_02_{platform_name:s}_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{creation_time:%Y%m%d%H%M%S}Z"
#                    W_XX-EUMETSAT-mos,IASI,DBNet+metopb+mos_C_EUMS_20161110084055_IASI_PW3_02
# iasi_file_pattern = "W_XX-EUMETSAT-{ears_station:3s},IASI,DBNet+{platform_name:6s}+{ears_station2:3s}_C_EUMS_{start_time:%Y%m%d%H%M%S}_IASI_PW3_02"
# Example:
# W_XX-EUMETSAT-kan,iasi,metopb+kan_C_EUMS_20170419171127_IASI_PW3_02_M01_20170419164952Z_20170419170214Z.hdf
IASI_FILE_PATTERN = "W_XX-EUMETSAT-{ears_station:3s},iasi,{platform_name2:6s}+{ears_station2:3s}_C_EUMS_{processing_time:%Y%m%d%H%M%S}_IASI_PW3_02_{platform_name:3s}_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z"


VAR_NAMES_AND_TYPES = {
    "temp": ("air_temperature_ml", "f"),
    "tdew": ("dew_point_temperature", "f"),
    "qspec": ("specific_humidity_ml", "f"),
    "pres": ("air_pressure", "f"),
    "ozone": ("mass_fraction_of_ozone_in_air", "f"),
}


SURFACE_VAR_NAMES_AND_TYPES = {
    "skin_temp": ("surface_temperature", "f"),
    "topo": ("surface_elevation", "f"),
}


# <attribut suffix in input structure>:  (<attribute name in file>)
ATTRIBUTE_NAMES = {
    "standardname": ("standard_name"),
    "longname": ("long_name"),
    "units": ("units"),
    "validrange": ("valid_range"),
    "gain": ("scale_factor"),
    "intercept": ("add_offset"),
}

PLATFORMS = {
    "M01": "Metop-B",
    "M02": "Metop-A",
    "M03": "Metop-C",
    "METOPA": "Metop-A",
    "METOPB": "Metop-B",
    "METOPC": "Metop-C",
    "metopa": "Metop-A",
    "metopb": "Metop-B",
}

_DEFAULT_LOG_FORMAT = "[%(levelname)s: %(asctime)s : %(name)s] %(message)s"
_DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
