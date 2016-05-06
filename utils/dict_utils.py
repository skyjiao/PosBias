# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'

import pickle
import os, sys

def reverse_dict(mydict):
    """swap key value of a dictionary
    """
    if sys.version_info >= (3, 0):
        return dict((value, key) for key, value in mydict.items())
    else:
        return dict((value, key) for key, value in mydict.iteritems())

def save_dict(mydict, path, name, verbose = False):
    if verbose:
        print("dumping dict %s"%name)
    pickle.dump(mydict, open(os.path.join(path, name), "wb"))

def load_dict(path, name):
    return pickle.load(open(os.path.join(path, name), "rb"))
