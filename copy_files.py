
import os
from shutil import copy2
from typing import List

def main(input_dir: str, suffix_list: List[str], output_dir: str):
    input_flist_path = os.path.join(input_dir, '0.filelist')
    
    if os.path.isfile(input_flist_path):
        flist = read_1D_list(input_flist_path)
    else:
        flist = to_filelist(input_dir)

    files_to_output = [
        fname
        for fname in flist
        if len([s for s in suffix_list if fname.endswith(s)]) > 0
    ]
    print(f'{len(files_to_output)} files will be copied.')

    for fname in files_to_output:
        orig_file_path = os.path.join(input_dir, fname)
        dest_file_path = os.path.join(output_dir, fname)

        copy2(orig_file_path, dest_file_path)

    flist = to_filelist(output_dir)
    print(f'{len(flist)} files are saved in {output_dir}.')
    
def to_filelist(dir_path):
    '''Return a file list in a given directory'''
    l1 = os.listdir(dir_path)
    l2= [a for a in l1 if not a.startswith('.')]
    flist = [a for a in l2 if not a.startswith('0')]

    with open(os.path.join(dir_path, '0.filelist'), 'w') as f:
        print('itemnum: '+str(len(flist)), file=f)
        print('\n'.join(flist), file=f)
    return flist


def read_1D_list(path, apply_func=None):
    if apply_func == None:
        apply_func = lambda s: s
    with open(path, 'r') as f:
        return [apply_func(l[:-1]) for l in f if not l.startswith('itemnum')]
    
if __name__ == '__main__':
    import sys
    input_dir = sys.argv[1]
    suffix_list = sys.argv[2].split(':')
    output_dir = sys.argv[3]

    main(input_dir, suffix_list, output_dir)
