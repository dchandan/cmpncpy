#!/usr/bin/env python

import re
from setuptools import find_packages, setup

if __name__ == "__main__":

    VERSIONFILE = "cmpnc/_version.py"
    verstrline = open(VERSIONFILE, "rt").read()
    VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        verstr = mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))


    setup(name='cmpnc',
          version=verstr,
          description="Compare the data in two netCDF files",
          author="Deepak Chandan",
          author_email="dchandan@atmosp.physics.utoronto.ca",

          license="MIT",

          classifiers=[
              'Development Status :: 5 - Production/Stable',

              'License :: OSI Approved :: MIT License',

              'Intended Audience :: Scientists',

              'Programming Language :: Python :: 3.5',
          ],

          keywords='netcdf',
          packages=find_packages(),

          entry_points={
              'console_scripts': [
                  'cmpnc = cmpnc.cmpnc:main',
              ]
          },
          )
