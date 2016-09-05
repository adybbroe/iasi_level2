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

"""Setup file for the iasi-level2 extraction and conversion library. 
"""

try:
    with open('./README.md', 'r') as fd:
        long_description = fd.read()
except IOError:
    long_description = ''


from setuptools import setup
import imp

SHORT_DESC = ("EARS-IASI level-2 format converter")

version = imp.load_source(
    'iasi_level2.version', 'iasi_level2/version.py')

setup(name='iasi-level2',
      version='0.1.0',
      description=SHORT_DESC,
      author='Adam Dybbroe',
      author_email='adam.dybbroe@smhi.se',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Science/Research',
                   'License :: OSI Approved :: GNU General Public License v3 ' +
                   'or later (GPLv3+)',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python',
                   'Topic :: Scientific/Engineering'],
      # url='https://github.com/adybbroe/py...',
      # download_url="https://github.com/adybbroe/py....
      long_description=long_description,
      license='GPLv3',

      packages=['iasi_level2', ],
      package_data={},

      # Project should use reStructuredText, so ensure that the docutils get
      # installed or upgraded on the target machine
      install_requires=['docutils>=0.3',
                        'numpy>=1.5.1',
                        'h5py',
                        'netCDF4',
                        'pyresample',
                        'pyorbital >= v0.2.3'],

      # test_requires=["mock"],
      scripts=['iasi_level2/ftpget_iasi_l2.py', ],
      # data_files=[('etc', ['etc/iasi_level2_config.cfg.template']),
      #            ],
      # test_suite='tests.suite',
      # tests_require=[],
      zip_safe=False
      )
