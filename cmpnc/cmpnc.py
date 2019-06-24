#!/usr/bin/env python

import sys
import os.path as osp
from netCDF4 import Dataset
import numpy as np
import argparse
import multiprocessing
from multiprocessing import Process, Queue
from ._version import __version__


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


def customAssert(a, b, msg, conterr=False):
    """
    Args:
        conterr: if True, then continues with error, otherwise
                 raises an exception
    """
    retval = 0
    if (a != b):
        if not conterr:
            raise AssertionError(msg)
        else:
            print(bcolors.FAIL + msg + bcolors.ENDC)
            retval = 1

    return retval


PASS_MSG = bcolors.OKGREEN + "        PASS: " + bcolors.ENDC + "{0}"
FAIL_MSG = bcolors.FAIL    + "        FAIL: " + bcolors.ENDC + "{0}"
SKIP_MSG = bcolors.WARNING + "        SKIPPING: " + bcolors.ENDC + "{0}"


def check_files_exist(file1, file2=""):
    exts1 = False
    exts2 = True
    exts1 = osp.isfile(file1)
    if file2 != "":
        exts2 = osp.isfile(file2)

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
    for i in range(sti, edi + 1):
        try:
            assert(np.allclose(var1[i], var2[i]))
        except AssertionError:
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


def parcomp(fname1, fname2, vars_, verbose, done_queue=None):
    """
    A "worker" function that is called to compare a variable which has an
    unlimited dimension between two steps along the unlimted dimension
    Args:
        fname1(str) : name of the first file
        fname2(str) : name of the second file
        vars(list)  : names of variables to compare
        done_queue(Queue) : If supplied, then the return value is returned 
                      through this queue
    Returns:
        True, if the check succeeded, False otherwise. If done_queue is present
        then the success value is returned there instead
    """
    ncf1 = Dataset(fname1, "r")
    ncf2 = Dataset(fname2, "r")

    success = True
    failed_vars = []
    skipped_vars = []
    for var in vars_:
        var1 = ncf1.variables[var]
        var2 = ncf2.variables[var]

        var_dtype = var1.dtype
        # Checking 'non-string' variables only
        if (var_dtype != np.dtype('S1')):
            try:
                assert(np.allclose(var1[Ellipsis], var2[Ellipsis]))
                if verbose:
                    print(PASS_MSG.format(var))
            except AssertionError:
                success = False
                failed_vars.append(var)
                if verbose:
                    print(FAIL_MSG.format(var))
        else:
            if verbose:
                print(SKIP_MSG.format(var))
            skipped_vars.append(var)

    ncf1.close()
    ncf2.close()
    if done_queue:
        done_queue.put((success, failed_vars, skipped_vars))
    else:
        return (success, failed_vars, skipped_vars)


def compare_variables(fname1, fname2, unlimdim, ulen, verbose, summary, conterr,
                      nprocsg):
    """
    Args:
        fname1(str) : name of the first file
        fname2(str) : name of the second file
        unlimdim(str): name of the unlimited dimension
        ulen(int)   : length of the unlimited dimension
        verbose(bool) : prints output to screen if True
        summary(bool) : if True, prints the number of variables passed/fail at the end
        conterr(bool) : if True, then continue checking despite any errors
    """

    if conterr:
        # If user has asked to continue with errors, then we have to print summary
        # to say what passed and what failed
        summary = True

    if verbose:
        print("Comparing Variables")

    ncf1 = Dataset(fname1, "r")
    ncf2 = Dataset(fname2, "r")

    vars1 = ncf1.variables
    vars2 = ncf2.variables

    # Categorizing variables into those with and without unlimited dimensions
    vars_u = []  # variables with unlimited dimension
    vars_ = []  # variables without unlimited dimensions

    for varname, var in vars1.items():
        if unlimdim in var.dimensions:
            vars_u.append(varname)
        else:
            vars_.append(varname)

    customAssert(len(vars1), len(vars2), "Number of variables different in files",
                 conterr=conterr)
    customAssert(set(list(vars1.keys())), set(list(vars2.keys())),
                 "Variables different in files", conterr=conterr)

    ncf1.close()
    ncf2.close()

    failed_vars = []
    skipped_vars = []
    all_okay = True

    # ====================================================================================
    if verbose:
        print("    Variables without unlimited dimension:")
    numvars = len(vars_)

    nprocs = nprocsg

    if nprocs > numvars:
        nprocs = numvars

    ipp = numvars // nprocs

    done_queue = Queue()

    procs = []
    for i in range(nprocs):
        sti = ipp * i
        if i < (nprocs - 1):
            edi = ipp * (i + 1)
        else:
            edi = numvars
        procs.append(Process(target=parcomp, args=(fname1, fname2, vars_[sti:edi],
                                                   verbose, done_queue)).start())

    for i in range(nprocs):
        s = done_queue.get()
        all_okay = all_okay and s[0]
        for item in s[1]:
            failed_vars.append(item)
        for item in s[2]:
            skipped_vars.append(item)
    # ====================================================================================

    # ====================================================================================
    if unlimdim is not "None":
        if verbose:
            print("    Variables WITH unlimited/time dimension:")

        numvars = len(vars_u)
        if (ulen < nprocs) and (numvars > 4):
            # This branch means that the number of steps along the unlimited dimension
            # is not sufficiently large enough to decompose along that dimension, but
            # the number of variables is large enough that we can partition the variables
            # among different processors.
            nprocs = nprocsg
            ipp = numvars // nprocs
            if not ipp > 1:
                while (numvars / nprocs < 1) and (nprocs > 0):
                    nprocs //= 2
                    ipp = numvars // nprocs
            done_queue = Queue()
            procs = []
            if verbose:
                print(bcolors.HEADER + "Using {0} processes".format(nprocs) + bcolors.ENDC)
            for i in range(nprocs):
                sti = ipp * i
                edi = ipp * (i + 1) if (i < nprocs - 1) else ipp * (i + 1) + numvars % nprocs
                p = Process(target=parcomp, args=(fname1, fname2, vars_u[sti:edi], verbose, done_queue))
                procs.append(p)
                p.start()

            for i in range(nprocs):
                s = done_queue.get()
                all_okay = all_okay and s[0]
                for item in s[1]:
                    failed_vars.append(item)
                for item in s[2]:
                    skipped_vars.append(item)

            for p in procs:
                p.join()
        else:
            for var in vars_u:
                if ulen >= nprocs:
                    # If there are enough steps in the unlimited dimensions then lets paritiion
                    # along that dimension
                    success = compare_umlim_var(fname1, fname2, var, ulen, nprocs)
                else:
                    # Just serially compare the variables.....
                    success = work(fname1, fname2, var, 0, ulen - 1)

                if not success:
                    all_okay = False
                    failed_vars.append(var)
                    if verbose:
                        print(FAIL_MSG.format(var))
                else:
                    if verbose:
                        print(PASS_MSG.format(var))
        # ====================================================================================
        nvars = len(vars_) + len(vars_u)
        nfailed = len(failed_vars)
        nskipped = len(skipped_vars)
        npassed = nvars - nfailed - nskipped

    if summary:
        print("")
        print(bcolors.HEADER + "SUMMARY" + bcolors.ENDC)
        print("File 1 : {0}".format(fname1))
        print("File 2 : {0}".format(fname2))
        print("Number of variables passed : {0}".format(npassed))
        print("Number of variables failed : {0}".format(nfailed))
        print("Number of variables skipped: {0}\n".format(nskipped))

    if not all_okay:
        print("These variables are not the same between the two files:")
        print(failed_vars)
        print(bcolors.FAIL + "Files are not identical\n" + bcolors.ENDC)
        sys.exit(1)
    else:
        if verbose:
            print(bcolors.OKGREEN + "Files appear to be identical\n" + bcolors.ENDC)


def compare_dimensions(dims1, dims2, verbose, conterr):
    """
    Compare the dimensions from the two variables.
    Args:
        dims1, dims2: dimensions from the two files
        verbose(bool) : if True, produce more output
        conterr(bool) : if True, then continue checking despite any errors
    """
    customAssert(len(dims1), len(dims2), "Number of dimensions different in files", conterr=conterr)
    customAssert(set(list(dims1.keys())), set(list(dims2.keys())), "Dimensions in files different",
                 conterr=conterr)
    if verbose:
        print("Comparing Dimensions")
        print("    Both files have {0} dimensions".format(len(dims1)))

    for k in list(dims1.keys()):
        retval = customAssert(len(dims1[k]), len(dims2[k]),
                              "    Length od dimension {0} differs between files".format(k),
                              conterr=conterr)
        if verbose and (retval == 0):
            print("    Dimension {0} ".format(k) + bcolors.OKBLUE + "identical" + bcolors.ENDC)


def get_unlim_dimension(dimensions):
    unlimdim = None
    for d in dimensions:
        if dimensions[d].isunlimited():
            unlimdim = d

    return unlimdim


def compare_attributes(ncf1, ncf2, verbose, conterr):
    att1 = ncf1.ncattrs()
    att2 = ncf2.ncattrs()
    continue_comparing = True
    try:
        customAssert(len(att1), len(att2), "Number of attributes different")
        customAssert(att1, att2, "Attributes different in files")
    except AssertionError:
        if conterr:
            continue_comparing = False
        else:
            raise

    if continue_comparing:
        if verbose:
            print("Comparing Attributes")
            print("    Both files have {0} attributes".format(len(att1)))

        for att in att1:
            retval = customAssert(getattr(ncf1, att), getattr(ncf2, att),
                                  "    Attribute {0} differs between the files".format(att),
                                  conterr=conterr)
            if verbose and (retval == 0):
                print("    Attribute {0} ".format(att) + bcolors.OKBLUE + "identical" +
                      bcolors.ENDC)


def start_compare(file1, file2, verbose, summary, conterr, nprocs, args):
    check_files_exist(file1, file2)

    ncf1 = Dataset(file1, "r")
    ncf2 = Dataset(file2, "r")

    dims1 = ncf1.dimensions
    dims2 = ncf2.dimensions

    unlimdim = get_unlim_dimension(dims1)
    if unlimdim is not None:
        ulen = len(ncf1.dimensions[unlimdim])
        if verbose:
            print("Found unlimited dimension {0} of length {1}".format(unlimdim, ulen))
    else:
        try:
            ulen = len(ncf1.dimensions["time"])
            unlimdim = "time"
        except KeyError:
            unlimdim = "None"
            ulen = 0

    compare_dimensions(dims1, dims2, verbose, conterr)
    if not args.ignore_attributes:
        compare_attributes(ncf1, ncf2, verbose, conterr)

    ncf1.close()
    ncf2.close()

    compare_variables(file1, file2, unlimdim, ulen, verbose, summary, conterr, nprocs)


def main():
    parser = argparse.ArgumentParser(prog="{0}".format(osp.basename(__file__)),
                                     description=("Compare two netCDF files to check if they"
                                                  " are identical"),
                                     epilog="Author: Deepak Chandan")
    parser.add_argument('files', type=str, nargs=2, help="two files to compare")
    parser.add_argument('-v', action='store_true', default=False, help="verbose output")
    parser.add_argument('-s', action='store_true', default=False, help="print summary of comparison")
    parser.add_argument('-k', action='store_true', default=False,
                        help="continue comparing files even after encountering errors")
    parser.add_argument('-n', type=int, default=multiprocessing.cpu_count(),
                        help="number of parallel processes to use during comparison")
    parser.add_argument('--ignore-attributes', action='store_true', default=False,
                        help="don't compare netCDF global attributes")
    parser.add_argument('--version', action='version', version='%(prog)s {0}'.format(__version__))

    args = parser.parse_args()

    files = args.files

    start_compare(*files, args.v, args.s, args.k, args.n, args)


if __name__ == "__main__":
    main()