#!/usr/bin/env python
import os
from os.path import expanduser, exists
import subprocess
import pynbody
import numpy as np
from pynbody import family

def slice_file(filename, output=None, block_size=10240): 
    """Take a Tipsy file and create four files of constant record length"""

    s = pynbody.load(filename)
    f = open(filename)
    f.seek(32)


    for fam, dtype in ((family.gas, s._g_dtype), 
                        (family.dm, s._d_dtype), 
                        (family.star, s._s_dtype)):
    
        if output is None:     
            outfile = filename+'_'+fam.name
        else: 
            outfile = output
        out = open(outfile, 'wb')
    
        to_read = dtype.itemsize*len(s[fam])
        print to_read
        while to_read > 0 :
            out.write(f.read(min(block_size, to_read)))
            to_read -= block_size
        out.close()
    f.close()


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



