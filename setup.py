#!/usr/bin/env python

from setuptools import find_packages, setup

if __name__ == "__main__":

    setup(name='cmpnc',
          version='1.0',
          description="Compare two netCDF files",
          author="Deepak Chandan",
          author_email="dchandan@atmosp.physics.utoronto.ca",

          license="MIT",

          classifiers=[
              'Development Status :: 5 - Production/Stable',

              'License :: OSI Approved :: MIT License',

              'Intended Audience :: Scientists',

              'Programming Language :: Python :: 2.7',
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.2',
              'Programming Language :: Python :: 3.3',
              'Programming Language :: Python :: 3.4',
              'Programming Language :: Python :: 3.5',
          ],

          keywords='netcdf',
          packages=find_packages(),

          entry_points={
              'console_scripts': [
                  'cmpnc = cmpnc:main',
              ]
          },
          )
