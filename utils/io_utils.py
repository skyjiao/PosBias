# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'

"""
I/O related utility functions:
- writing logs
- create/remove files/directionaries
"""

import os, sys
import shutil
import datetime

def update_progress(progress):
    if isinstance(progress, int):
        progress = float(progress)
    text = "\rProgress: {0}%".format(progress*100)
    sys.stdout.write(text)
    sys.stdout.flush()

def write_log(s, file, verbose=False):
    """
    Appends new line and writes message to file
    and prints it if required
    """
    file.write(s + '\n')
    if verbose:
        print(s, flush=True)

def make_backup(filename, append='~'):
    """
    makes a backup of <filename> (if existing) by appending
    character(s) <append>
    """
    try:
        shutil.copy(filename, filename + append)
    except:
        pass

def mkdirs_and_open(filename, *args, **kwargs):
    """
    Create dir of <filename> if not existing
    Open <filename> and backup the file if necessary

    Params:
    ------
    filename: str
        target file path
    """
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    backup = kwargs.pop('backup', False)
    append = kwargs.pop('append', '~')

    if backup:
        make_backup(filename=filename, append=append)

    return open(filename, *args, **kwargs)



