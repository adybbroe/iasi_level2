#!/usr/bin/env python3
"""Unit tests for the iasi_lcl2 file code."""
from iasi_level2.iasi_lvl2 import IasiLvl2


def test_iasi_lvl2():
    directory = "/data/lang/satellit2/polar/system_test_cases/iasi_l2"
    fname = "W_XX-EUMETSAT-lan,iasi,metopb+lan_C_EUMS_20230327093536_IASI_PW3_02_M01_"
    fname += "20230327091606Z_20230327092820Z.hdf"
    TESTFILE = f"{directory}/{fname}"
    l2p = IasiLvl2(TESTFILE)

    l2p.ncwrite()
    expected_fname = fname.replace(".hdf", "_vprof.nc")
    new_l2p = IasiLvl2(expected_fname)

    l2p.ncwrite(vprof=False)
    expected_fname = fname.replace(".hdf", "_vcross.nc")
    new_l2p = IasiLvl2(expected_fname)
