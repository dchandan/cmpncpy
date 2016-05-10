#!/usr/bin/env python

import os
import sys
import os.path as osp
from netCDF4 import Dataset
import numpy as np
import argparse
import multiprocessing
from multiprocessing import Process, Queue

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    def __init__(self, *args, **kwargs):
        pass

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


def work(fname1, fname2, var, sti, edi, done_queue=None):
    """
    A "worker" function that is called to compare a variable which has an
    unlimited dimension between two steps along the unlimted dimension
    Args:
        fname1(str) : name of the first file
        fname2(str) : name of the second file
        var(str)    : name of the variable
        sti(int)    : start index along the unlimted dimension
        edi(int)    : end index along the unlimited dimension
        done_queue(Queue) : If supplied, then the return value is returned 
                      through this queue
    Returns:
        True, if the check succeeded, False otherwise. If done_queue is present
        then the success value is returned there instead
    """
    ncf1 = Dataset(fname1, "r")
    ncf2 = Dataset(fname2, "r")

    var1 = ncf1.variables[var]
    var2 = ncf2.variables[var]
    
    success = True
    for i in range(sti, edi+1):
        try:
            assert(np.allclose(var1[i], var2[i]))
        except:
            success = False

    ncf1.close()
    ncf2.close()

    if done_queue:
        done_queue.put(success)
    else:
        return success


def compare_umlim_var(fname1, fname2, var, ulen, nprocs):
    """
    Compares a variable that has an unlimited dimension.
    Args:
        fname1(str) : name of the first file
        fname2(str) : name of the second file
        var(str)    : name of the variable
        ulen(int)   : length of the unlimited dimension
        nprocs(int) : number of parallel processes to use
    Returns:
        True, if the check succeeded, False otherwise
    """
    procs = []
    ipp = ulen // nprocs
    done_queue = Queue()
    for i in range(nprocs):
        sti = (ipp * i)   # The start index for the process
        # Calculating the appropriate stop index for the process
        if i != (nprocs - 1):
            edi = ipp * (i + 1) - 1
        else:
            edi = ulen - 1

        p = Process(target=work, args=(fname1, fname2, var, sti, edi, done_queue))
        procs.append(p)
        p.start()

    success = True
    for i in range(nprocs):
        success = success and done_queue.get()

    return success



def compare_variables(fname1, fname2, unlimdim, ulen, verbose):    
    """
    Args:
        fname1(str) : name of the first file
        fname2(str) : name of the second file
        unlimdim(str): name of the unlimited dimension
        ulen(int)   : length of the unlimited dimension
        verbose(bool) : prints output to screen if True.
    """
    PASS_MSG = bcolors.OKGREEN + "        PASS: " + bcolors.ENDC + "{0}"
    FAIL_MSG = bcolors.FAIL + "        FAIL: " + bcolors.ENDC + "{0}"

    if verbose: print("Comparing Variables")

    ncf1 = Dataset(fname1, "r")
    ncf2 = Dataset(fname2, "r")

    vars1 = ncf1.variables
    vars2 = ncf2.variables
    
    # Categorizing variables into those with and without unlimited dimensions
    vars_u = []  # variables with unlimited dimension
    vars_  = []  # variables without unlimited dimensions

    for varname, var in vars1.items():
        if unlimdim in var.dimensions:
            vars_u.append(varname)
        else:
            vars_.append(varname)

    assert_(len(vars1), len(vars2), "Number of variables different in files")
    assert_(list(vars1.keys()), list(vars2.keys()), "Variables different in files")

    failed_vars = []

    all_okay = True

    if verbose: print("    Variables without unlimited dimension:")
    for var in vars_:
        var1 = vars1[var]
        var2 = vars2[var]

        var_dtype = var1.dtype

        # Checking 'non-string' variables only
        if (var_dtype != np.dtype('S1')):
            try:
                assert(np.allclose(var1[Ellipsis], var2[Ellipsis]))
                if verbose: print (PASS_MSG.format(var))
            except:
                all_okay = False
                failed_vars.append(var)
                if verbose: print (FAIL_MSG.format(var))

        else:
            if verbose: print ("    Skipping check for this variable")

    ncf1.close()
    ncf2.close()
    
    if verbose: print("    Variables WITH unlimited dimension:")
    nprocs = multiprocessing.cpu_count()
    for var in vars_u:
        if ulen >= nprocs:
            success = compare_umlim_var(fname1, fname2, var, ulen, nprocs)
        else:
            success = work(fname1, fname2, var, 0, ulen-1)

        if not success:
            all_okay = False
            failed_vars.append(var)
            if verbose: print (FAIL_MSG.format(var))
        else:
            if verbose: print (PASS_MSG.format(var))

    if not all_okay:
        print("These variables are not the same between the two files:")
        print(failed_vars)
        print(bcolors.FAIL + "Files are not identical\n" + bcolors.ENDC)
        sys.exit(1)
    else:
        if verbose:
            print(bcolors.OKGREEN + "Files seem to be identical\n" + bcolors.ENDC)


def compare_dimensions(dims1, dims2, verbose):
    """
    Compare the dimensions from the two variables.
    Args:
        dims1, dims2: dimensions from the two files
        verbose: if True, produce more output
    """
    assert_(len(dims1), len(dims2), "Number of dimensions different in files")
    assert_(list(dims1.keys()), list(dims2.keys()), "Dimensions in files different")
    if verbose: 
        print("Comparing Dimensions")
        print(("  Both files have {0} dimensions".format(len(dims1))))
        
    for k in list(dims1.keys()):
        assert_(len(dims1[k]), len(dims2[k]), "Lengths not same for dimension {0}".format(k))
        if verbose: print(("  Dimension {0} same".format(k)))


def get_unlim_dimension(dimensions):
    unlimdim = None
    for d in dimensions:
        if dimensions[d].isunlimited():
            unlimdim = d
    return unlimdim



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

    unlimdim = get_unlim_dimension(dims1)
    ulen = len(ncf1.dimensions[unlimdim])
    if verbose: print("Found unlimited dimension {0} of length {1}".format(unlimdim, ulen))

    compare_dimensions(dims1, dims2, verbose)
    compare_attributes(ncf1, ncf2, verbose)

    ncf1.close()
    ncf2.close()

    compare_variables(file1, file2, unlimdim, ulen, verbose)


def main():
    parser = argparse.ArgumentParser(prog="{0}".format(osp.basename(__file__)))

    parser.add_argument('files', type=str, nargs=2, help="two files to compare")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")

    args = parser.parse_args()

    files = args.files

    compare(*files, verbose=args.v)    


if __name__ == "__main__":
    main()