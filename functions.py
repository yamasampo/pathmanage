""" Functions to help dealing with paths in tree structure. """

import os
import fnmatch

def gen_find_file(top, filepat):
    '''Find all filenames in a directory tree that match a shell wildcard pattern'''
    for path, _, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)

def gen_find_dir(top, filepat):
    """ Obtain all directory names in a directory tree that contain files 
    matching to a shell wildcard pattern. This function allows you to process
    linearly through directories in tree structured file organization. """
    for path, _, filelist in os.walk(top):
        if len(fnmatch.filter(filelist, filepat)) > 0:
            yield path
            