"""This script splits contents of one file into multiple files. 


Example
-------
An input file contains

>>ID1
>1
data
>2
data

>>ID2
>1
data
>2
data
>3
data
...
>1000
data


Then this script will output three files. 
One contains lines for the items for ID1
[outfile 1]
>>ID1
>1
data
>2
data

The other two contain lines for the items for ID2. 
[outfile 2]
>>ID2
>1
data
>2
data
>3
data
>500
data

[outfile 3]
>>ID2
>501
data
...
>1000
data

----------------

Change output files if
- the first level ids change
(i.e., ID1 -> ID2)

- the number of items within the first level id exceed a given threshold
if 500 is given as this number, 
the items of ID2 will be split into two files. 
The output files will have the first level ID.


This process needs the following steps;

- tell whether the current line is between the ids at the first level
- count the number of items inside the first level id.

Data can be output with a modification. Currently, this script supports the 
modification to remove redundant characters that appear at the beginning of the 
item. In this case, the order of members must be consistent across all the lines. 
"""

import os
import bz2

from typing import List
from datetime import datetime

from pathmanage import __version__
from nothingspecial.text import to_filelist
from nothingspecial.keeplog import save_proc_setting_as_file

def compress_all_files(input_dir, suffix, output_dir):
    """Read the entire file on memory and save into bzip file. 
    For large file, we can add another option for read and output in chunk. 
    lzma may be a good module for this possibility. 
    This web article has a good demonstration:
    https://towardsdatascience.com/all-the-ways-to-compress-and-archive-files-in-python-e8076ccedb4b 

    """
    start = datetime.now().isoformat()
    input_args = {
        'input_dir': input_dir, 
        'suffix': suffix,
        'output_dir': output_dir
    }
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
        
    log_file = os.path.join(output_dir, '0.log.txt')
    with open(log_file, 'a') as log_fh:
        save_proc_setting_as_file(
            log_fh, 
            package_name = 'pathmanage', 
            proc_desc = 'Compress all files in an input directory.', 
            start_time = start, 
            separator = ' = ',
            package_version = __version__, 
            **input_args
        )
        print('\n[Process log]', file=log_fh)

    in_fnames = [
        fname 
        for fname in os.listdir(input_dir) 
        if fname.endswith(suffix)
    ]
    in_fpaths = [os.path.join(input_dir, fname) for fname in in_fnames]
    out_fnames = [
        fname + '.bz2'
        for fname in in_fnames
    ]
    out_fpaths = [os.path.join(output_dir, fname) for fname in out_fnames]

    for in_fpath, out_fpath in zip(in_fpaths, out_fpaths):
        with open(in_fpath, 'rb') as in_fh, bz2.open(out_fpath, 'wb') as out_fh:
            out_fh.write(in_fh.read())

            with open(log_file, 'a') as log_fh:
                print(f'\nOutput {out_fpath}')

    out_flist = to_filelist(output_dir)

    with open(log_file, 'a') as log_fh:
        print(f'A total of {len(out_flist)} files are saved.')

    return out_flist

def split(
        input_file, 
        output_file_prefix, 
        max_item_num    : int, 
        item_prefixes   : List[str] = [],
        item_separator  : str = '\t'
        ):
    
    start = datetime.now().isoformat()

    # In case that user wants to remove prefixes that appear consistently 
    # accross lines
    member_num = len(item_prefixes)
    contents = [] # Strings in this list will be output in the same file
    output_file_id = 1
    first_level_id = ''
    second_level_item_num = 0
    total_line_count = 0
    total_file_count = 0

    # Write input arguments to a log file
    input_args = {
        'input_file': input_file, 
        'output_file_prefix': output_file_prefix, 
        'max_item_num': max_item_num, 
        'item_prefixes': item_prefixes, 
        'item_separator': item_separator
    }
    log_file = get_output_file_path(output_file_prefix, 'log')
    with open(log_file, 'a') as log_fh:
        save_proc_setting_as_file(
            log_fh, 
            package_name = 'pathmanage', 
            proc_desc = 'Split a huge file into multiple file.', 
            start_time = start, 
            separator = ' = ',
            package_version = __version__, 
            **input_args
        )
        print('\n[Process log]', file=log_fh)

    current_output_file_path = get_output_file_path(
        output_file_prefix, output_file_id
    )

    # Get a file handler of the input file
    with open(input_file, 'r') as input_fh:
        # Access to each line from the top
        for l in input_fh:
            # Cut empty characters at the both ends.  
            line = l.strip()
            
            if line == '':
                continue

            # If the current line indicates the first level ID
            if line.startswith('>>'):
                # If the current line is not the first item
                if first_level_id != '':
                    # Output contents in output_lines to a file. 
                    current_output_file_path, contents = \
                        save_to_file_and_switch_output_file(
                            contents, current_output_file_path, 
                            output_file_prefix,
                            log_file, 
                            comment_line = ''
                        )
                    total_file_count += 1
                    second_level_item_num = 0
                
                first_level_id = line

            # If the current line indicates the second level ID
            elif line.startswith('>'):
                # If the number of second level ids is greater than a given value
                if second_level_item_num >= max_item_num:
                    # Output contents in output_lines to a file. 
                    current_output_file_path, contents = \
                        save_to_file_and_switch_output_file(
                            contents, current_output_file_path, 
                            output_file_prefix, 
                            log_file,
                            comment_line = ''
                        )
                    # If contents for one first level id are split into several,
                    # Add the first level ID at the beginning in the next file. 
                    contents = [first_level_id]
                    total_file_count += 1
                    second_level_item_num = 0

                second_level_item_num += 1
            
            # If the current line indicates data
            else:
                if member_num > 0:
                    # Split a line to 
                    parts = line.split()
                    # Check if the number of items matches to the expectation
                    assert len(parts) == member_num

                    modified_items = [
                        ''.join(item.split(exp_pref)[1:]) # Remove prefix
                        for exp_pref, item in zip(item_prefixes, parts)
                        if item.startswith(exp_pref)
                     ]
                    if len(modified_items) != member_num:
                        msg = 'There is one or more items that do not start '\
                              f'with expected prefixes: \nObserved items: '\
                              f'{parts}\nExpected prefixes: {item_prefixes}'
                        with open(log_file, 'a') as log_f:
                            print(msg, file=log_f)
                        raise ValueError(msg)
                    
                    line = item_separator.join(modified_items)

            contents.append(line)
            total_line_count += 1
    
    # Output the remaining items
    current_output_file_path, contents = save_to_file_and_switch_output_file(
        contents, 
        current_output_file_path, 
        output_file_prefix, 
        log_file,
        comment_line = ''
    )
    total_file_count += 1
    
    with open(log_file, 'a') as log_f:
        print(f'A total of {total_line_count} (except empty lines) lines are '\
              f'recognized.', file=log_f)
        print(f'These lines are saved separately into {total_file_count} files.', 
              file=log_f)

    return total_line_count, total_file_count

def save_to_file_and_switch_output_file(
        contents                    : List[str], 
        current_output_file_path    : str,
        output_file_prefix          : str, 
        log_file                    : str, 
        comment_line                : str = ''
        ):
    # Output the contents to the current file
    with open(current_output_file_path, 'w') as f:
        if comment_line != '':
            print(comment_line, file=f)

        print('\n'.join(contents), file=f)

    with open(log_file, 'a') as log_f:
        print(f'Save contents to {current_output_file_path}.\n', file = log_f)
    
    # Get the next output file path
    curr_output_file_id = get_output_file_id_from_path(current_output_file_path)
    next_id = curr_output_file_id + 1
    next_output_file_path = get_output_file_path(output_file_prefix, next_id)

    # Empty the contents
    contents = []
    return next_output_file_path, contents

def get_output_file_path(output_file_prefix, output_file_id):
    output_file_path = output_file_prefix + f'_{output_file_id}.txt'

    # Check if the file does not exist
    if os.path.isfile(output_file_path):
        raise FileExistsError(output_file_path)
    
    return output_file_path

def get_output_file_id_from_path(file_path):
    return int(file_path.split('_')[-1].split('.')[0])

if __name__ == '__main__':
    import sys
    mode = sys.argv[1]

    if mode not in {'split', 'compress_all'}:
        raise ValueError('Please choose "split" or "compress_all"')
    
    if mode == 'split':
        split(
            sys.argv[2], 
            sys.argv[3], 
            int(sys.argv[4]), 
            sys.argv[5].split(':'),
            item_separator = '\t'
        )

    else:
        compress_all_files(
            sys.argv[2], sys.argv[3], sys.argv[4]
        )

