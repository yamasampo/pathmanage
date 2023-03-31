import os
import zipfile

def get_file_list_from_zipped_folder(zip_file_path):
    # Get folder name without an extention
    fname = os.path.basename(zip_file_path).split('.zip')[0]

    # Get a path to list file
    flist_fname = f'{fname}/0.filelist'

    # Open zip file
    with zipfile.ZipFile(zip_file_path) as z:
        # Open list file
        with z.open(flist_fname) as f:
            # Initialize list to collect file names
            flist = []
            # For each line
            for l in f:
                # Convert bytes type to string
                line = l.decode('utf-8')
                # If the line starts with "itemnum"
                if line.startswith('itemnum'):
                    # Get the number of items that follow in subsequent lines
                    exp_num = int(line.split('itemnum:')[1].strip())
                    
                else:
                    # Append line
                    flist.append(line.strip())
            # Check if the item number is the same as the expected
            assert len(flist) == exp_num
            
    return flist

def gen_lines_in_files_in_zipped_folder(zip_file_path, fname):
    """Generates each line from a file in the zipped folder. """
    # Open zip file
    with zipfile.ZipFile(zip_file_path) as z:
        # Open list file
        with z.open(fname) as f:
            for l in f:
                yield l.decode('utf-8')

