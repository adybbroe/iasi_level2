#!/usr/bin/env python3
"""Unit tests for the iasi_lcl2 file code."""
from iasi_level2.iasi_lvl2 import IasiLvl2


def test_iasi_lvl2():
    directory = "/home/a002216/data/tests"
    fname = "W_XX-EUMETSAT-lan,iasi,metopb+lan_C_EUMS_20230327093536_IASI_PW3_02_M01_20230327091606Z_20230327092820Z.hdf"
    TESTFILE = f"{directory}/{fname}"
    l2p = IasiLvl2(TESTFILE)
    l2p.ncwrite()
    l2p.ncwrite(vprof=False)
    raise ValueError(l2p.nc_filename)
    that = IasiLvl2(l2p.nc_filename)
