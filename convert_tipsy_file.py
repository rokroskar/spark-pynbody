#!/usr/bin/env python
import os
from os.path import expanduser, exists
import subprocess
import pynbody
import numpy as np
from pynbody import family

def slice_file(filename, output=None): 
    """Take a Tipsy file and create four files of constant record length"""
    from fractions import gcd

    # get the header to extract particle partition sizes
    s = pynbody.load(filename)

    prev_read_bytes = 32 # the header skip bytes

    for fam, dtype in ((family.gas, s._g_dtype), 
                        (family.dm, s._d_dtype), 
                        (family.star, s._s_dtype)):

        skip_bytes = prev_read_bytes 
        read_bytes = dtype.itemsize*len(s[fam])

        # need the greatest common denominator for block size
        block_size = gcd(read_bytes, skip_bytes)
        skip_blocks = skip_bytes/block_size
        read_blocks = read_bytes/block_size

        if output is None:
            outfile = filename+'_%s'%(fam.name)
        else: 
            outfile = output
    
        print "reading %s, skipping %d bytes, reading %d bytes"%(fam, skip_bytes, read_bytes)
        res = subprocess.check_output(['dd', 
                                        'if=%s'%filename, 
                                        'of=%s_%s'%(outfile, fam.name),
                                        'ibs=%d'%block_size,
                                        'obs=1m',
                                        'skip=%d'%skip_blocks,
                                        'count=%d'%read_blocks],
                                        stderr=subprocess.STDOUT)
        prev_read_bytes = read_bytes

        print res


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Cut a tipsy file into four parts: header, gas, dark, star')
    

    parser.add_argument('filename', metavar='f', type=str, help='input filename')

    parser.add_argument('--output', dest='output', type=str, default=None,
    					help='output filename (default: input_<type>)')
    
    args = parser.parse_args()

    filename = args.filename
    output   = args.output
	
    slice_file(filename, output)



