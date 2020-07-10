
import os
from shutil import copy2
from .functions import gen_find_file

def read_list(path):
	with open(path, 'r') as f:
	    return [l[:-1] for l in f if not l.startswith('itemnum')]
	    
def to_filelist(dir_path):
    '''Return a file list in a given directory'''
    l1 = os.listdir(dir_path)
    l2= [a for a in l1 if not a.startswith('.')]
    flist = [a for a in l2 if not a.startswith('0')]

    with open(os.path.join(dir_path, '0.filelist'), 'w') as f:
        print('itemnum: '+str(len(flist)), file=f)
        print('\n'.join(flist), file=f)
    return flist

def search_file(query_list, top_dir, file_pattern_formatter):
    for query_prefix in query_list:
        file_pat = file_pattern_formatter(query_prefix)
        found_cnt = 0

        for from_path in gen_find_file(top_dir, file_pat):
            yield from_path
            found_cnt += 1
        
        if found_cnt == 0:
            print(f'{file_pat} not found under {top_dir}.')

def copy_file(from_path, out_dir):
    fname = os.path.basename(from_path)
    to_path = os.path.join(out_dir, fname)
    copy2(from_path, to_path)

def make_formatter(prefix=None, suffix=None):
    if prefix == None:
        if suffix == None:
            return lambda s: f'*{s}*'
        return lambda s: f'*{s}{suffix}'
    
    if suffix == None:
        return lambda s: f'{prefix}{s}*'

    return lambda s: f'{prefix}{s}{suffix}'


def main(query_list_path, top_dir, out_dir, file_pattern_formatter):
    query_list = read_list(query_list_path)

    for from_path in search_file(query_list, top_dir, file_pattern_formatter):
        copy_file(from_path, out_dir)

    out_flist = to_filelist(out_dir)
    print(len(out_flist), f'files were copied to {out_dir}.')


if __name__ == '__main__':
    import sys
    query_list_path = sys.argv[1]
    top_dir = sys.argv[2]
    out_dir = sys.argv[3]
    prefix = None if sys.argv[4] == 'None' else sys.argv[4]
    suffix = None if sys.argv[5] == 'None' else sys.argv[5]
    formatter = make_formatter(prefix, suffix)

    input_data = f""" 
    [INPUT]
    query_list_path: {query_list_path}
    top_dir: {top_dir}
    out_dir: {out_dir}
    query_formatter: {formatter}
    """
    print(input_data)

    main(query_list_path, top_dir, out_dir, formatter)
