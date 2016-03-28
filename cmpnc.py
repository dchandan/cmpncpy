#!/usr/bin/env python

import sys
import os
import os.path as osp
from netCDF4 import Dataset
import numpy as np
import argparse

def assert_(a, b, msg):
    if (a != b): raise Exception(msg)

def check_files_exist(file1, file2=""):
    exts1 = False
    exts2 = True
    exts1 = osp.isfile(file1)
    if file2 != "": exts2 = osp.isfile(file2)

    if (exts1 and exts2):
        return True
    else:
        raise IOError("One or more input files don't exist")


def compare_dimensions(dim1, dim2, verbose):
    assert_(len(dim1), len(dim2), "Number of dimensions different in files")
    assert_(list(dim1.keys()), list(dim2.keys()), "Dimensions in files different")
    if verbose: 
        print("Comparing Dimensions")
        print(("  Both files have {0} dimensions".format(len(dim1))))
        
    for k in list(dim1.keys()):
        assert_(len(dim1[k]), len(dim2[k]), "Lengths not same for dimension {0}".format(k))
        if verbose: print(("  Dimension {0} same".format(k)))


def compare_variables(vars1, vars2, verbose):
    if verbose: print("Comparing Dimensions")
    assert_(len(vars1), len(vars2), "Number of variables different in files")
    assert_(list(vars1.keys()), list(vars2.keys()), "Variables different in files")

    failed_vars = []

    all_okay = True

    for var in list(vars1.keys()):
        if verbose:
            print(("  {0}".format(var)))
        var1 = vars1[var]
        var2 = vars2[var]

        var_dtype = var1.dtype

        # Checking 'non-string' variables only
        if (var_dtype != np.dtype('S1')):
            try:
                assert(np.allclose(var1[Ellipsis], var2[Ellipsis]))
                #assert((var1[Ellipsis] == var2[Ellipsis]).all())
                if verbose: print ("    Variable same")
            except AssertionError:
                # Maybe it failed because its a masked array...
                try:
                    assert(np.allclose(var1[Ellipsis].data, var2[Ellipsis].data))
                    assert(np.allclose(var1[Ellipsis].mask, var2[Ellipsis].mask))
                except AssertionError:
                    all_okay = False
                    failed_vars.append(var)
                    if verbose: print ("    Variable not same")
                except:
                    all_okay = False
                    failed_vars.append(var)
                    if verbose: print ("    Variable not same")

        else:
            if verbose: print ("    Skipping check for this variable")

    if not all_okay:
        print(failed_vars)
        raise Exception("Variable verification failed")



def compare_attributes(ncf1, ncf2, verbose):
    att1 = ncf1.ncattrs()
    att2 = ncf2.ncattrs()
    assert_(len(att1), len(att2), "Number of attributes different")
    assert_(att1, att2, "Attributes different in files")
    if verbose: 
        print("Comparing Attributes")
        print(("  Both files have {0} attributes".format(len(att1))))

    for att in att1:
        assert_(getattr(ncf1, att), getattr(ncf2, att), "Attribute {0} different in files".format(att))
        if verbose: print(("  Attribute {0} same".format(att)))


def compare(file1, file2, verbose):
    check_files_exist(file1, file2)
    ncf1 = Dataset(file1, "r")
    ncf2 = Dataset(file2, "r")

    dims1 = ncf1.dimensions
    dims2 = ncf2.dimensions

    vars1 = ncf1.variables
    vars2 = ncf2.variables

    compare_dimensions(dims1, dims2, verbose)
    compare_attributes(ncf1, ncf2, verbose)
    compare_variables(vars1, vars2, verbose)

    ncf1.close()
    ncf2.close()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="{0}".format(__file__))

    parser.add_argument('files', type=str, nargs=2, help="two files to compare")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")

    args = parser.parse_args()

    files = args.files

    compare(*files, verbose=args.v)