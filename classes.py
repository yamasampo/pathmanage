""" Classes for managing paths in tree structure. """

import os
import pickle
import numpy as np
import pandas as pd
from collections import defaultdict
from pathmanage.functions import gen_find_dir
from popgen.classes import Database, SFS
from popgen.statistics import tests

class DirMap(Database):
    """ Creates DirMap object which inherits from Database class. """

    def __init__(self, top, filepat, info_getter, columns=[], description=''):
        self.df, self._d = self.get_DirMap(
            top, filepat, info_getter, columns, description)
        self.description = description

    def gen_dir(self, sort_by='', ascending=True, **kwargs):
        filt_df =self.filter(sort_by, ascending, **kwargs)
        id_list = list(filt_df.index)

        for i in id_list:
            yield i, self._d[i]

    def get_DirMap(self, top, filepat, info_getter, 
                   columns=[], description='', **kwargs):
        """ Read through a given path tree from top directory and 
        collect information and corresponding directory paths into 
        a DataFrame and a dictionary, respectively.  
        
        Parameters
        ----------
        top: str
            A path to a top directory for reading directory tree
        filepat: str
            A string to find a target directory. A directory which contains 
            a file matching to filepat will be a target directory.
        info_getter: function
            A function to get information or data for the target directory.
            An absolute path of the target directory is an input of this function.
        columns: list
            A list of column names for output DataFrame. The length of columns
            has to be the same as the length of items in an output from 
            info_getter function.
        description: str
            A description of the dataset.

        Returns
        -------
        info_df: pd.DataFrame
            A matrix of outputs from info_getter
        dir_path_d: dict
            A dictionary of absolute paths of directories. keys of 
            the dir_path_d are corresponding to indices of info_df.
        """
        i = 0
        dir_path_d = {}
        df_data = []

        for dir_path in gen_find_dir(top, filepat):
            i += 1
            dir_path_d[i] = os.path.abspath(dir_path)
            data = info_getter(os.path.abspath(dir_path), **kwargs)
            df_data.append(data)

        info_df = pd.DataFrame(
            data, index=list(dir_path_d.keys), columns=columns)

        return info_df, dir_path_d

    def to_files(self, out_dir):
        # Save df
        df_path = os.path.join(
            out_dir, '{name}_{desc}_df.csv'.format(
                desc=self.description, name=type(self).__name__)
            )
        self.df.to_csv(df_path, index=False)

        # Save data
        pickle_path = os.path.join(
            out_dir, '{name}_{desc}_d.pickle'.format(
                desc=self.description, name=type(self).__name__)
            )
        with open(pickle_path, 'wb') as f:
            pickle.dump(self._d, f)

class SFSDirMap(DirMap):
    """ Creates SFSDirMap object which inherits from DirMap class. 
    This object is composed of a DataFrame containing dataset info and SFS data,
    and a dictionary containing corresponding a directory path for the dataset. """

    def __init__(self, csv_path='', pickle_path='', top='', sfs_format='', 
                 site_type_pos=None, scale=100,
                 description='', gene_list=[], gene_match_func=None):
        if csv_path:
            self.load_DirMap_from_files(
                csv_path, pickle_path)
        else:
            self.df, self._d = self.get_DirMap(
                top, sfs_format, site_type_pos, scale=scale, 
                description=description, 
                gene_list=gene_list, gene_match_func=gene_match_func
            )
        self.description = description

    def gen_sfs_dir(self, sort_by='', ascending=True, **kwargs):
        filt_df =self.filter(sort_by, ascending, **kwargs)
        id_list = list(filt_df.index)

        for i in id_list:
            sfs_id = filt_df.loc[i, 'sfs_id']

            yield self.df.loc[i], self._d[sfs_id]['sfs_dir']

    def gen_sfs_table(self, sort_by='', ascending=True, **kwargs):
        filt_df =self.filter(sort_by, ascending, **kwargs)
        id_list = list(filt_df.index)

        for i in id_list:
            sfs_id = filt_df.loc[i, 'sfs_id']
            
            yield self.df.loc[i], self._d[sfs_id]['sfs_table']

    @staticmethod
    def _sfs_info_formatter(sfs_data_dir, site_type_pos):
        site_type = sfs_data_dir.split('/')[site_type_pos]

        if site_type == 'CDS':
            cod_type = sfs_data_dir.split('/')[site_type_pos+2]
            aa = sfs_data_dir.split('/')[site_type_pos+3].split('_')[0]
            species = sfs_data_dir.split('/')[site_type_pos+6][:2]
            geneset = sfs_data_dir.split('/')[site_type_pos+5]

        else:
            cod_type = -9
            aa = -9
            species = sfs_data_dir.split('/')[site_type_pos+4][:2]
            geneset = sfs_data_dir.split('/')[site_type_pos+3]

        chr_name = sfs_data_dir.split('/')[site_type_pos+1].split('_')[1]
        filt_code = sfs_data_dir.split('/')[site_type_pos+1].split('_')[2]

        try:
            trim = sfs_data_dir.split('/')[site_type_pos+1].split('_')[3]
        except:
            trim = -9

        return {
            'site_type': site_type,
            'cod_type': cod_type,
            'aa': aa,
            'species': species,
            'chr': chr_name,
            'filt_code': filt_code,
            'trim': trim,
            'geneset': geneset
        }
    def load_DirMap_from_files(self, csv_path, pickle_path):
        """ Load data from csv and pickle files if data does not exist 
        in the object. """
        if hasattr(self, 'df'):
            raise AttributeError('"df" attribute exists. Loading failed.')

        if hasattr(self, '_d'):
            raise AttributeError('"_d" attribute exists. Loading failed.')

        self.df = pd.read_csv(csv_path)
        with open(pickle_path, 'rb') as f:
            self._d = pickle.load(f)

    def get_DirMap(self, top, sfs_format, site_type_pos, scale=100, 
                   description='', gene_list=[], gene_match_func=None):

        if sfs_format == 'gbgSFS':
            filepat = 'estimated_SFS+fixations_of_each_gene_0.txt'

        elif sfs_format == 'df':
            filepat = 'SFS_bootstrap_data.csv'

        else:
            raise Exception('Wrong sfs_format. Please input "gbgSFS" or "df".')

        i = 0
        dir_path_d = defaultdict(dict)

        site_type_list = []
        cod_type_list = []
        aa_list = []
        species_list = []
        chr_list = []
        filt_code_list = []
        trim_list = []
        geneset_list = []
        comp_str_list = []
        comp_id_list = []
        n1_list = []
        n2_list = []
        total_n_list = []
        mean1_list = []
        mean2_list = []
        mwu_z_list = []
        mwu_p_list = []
        sfs_id_list = []

        comp_mutations = [
            '1_GA_AG', '2_CT_TC', '3_SW_WS', '4_TA_AT', 
            '5_CG_GC', '6_WW_SS', '7_CA_AC', '8_GT_TG'
        ]

        amino_acids_2f = 'FYHQNKDECs'
        mutations = ['TC', 'TC', 'TC', 'AG', 'TC', 'AG', 'TC', 'AG', 'TC', 'TC']
        mutation_d_2f = dict(zip(amino_acids_2f, mutations))

        aa1 = 'FLIMVSPTAY*HQNKDECWrGs'
        aa3 = ['Phe', 'Leu', 'Ile', 'Met', 'Val', 'Ser4', 'Pro', 
            'Thr', 'Ala', 'Tyr', 'Stop', 'His', 'Gln', 'Asn', 'Lys', 
            'Asp', 'Glu', 'Cys', 'Trp', 'Arg', 'Gly', 'Ser2']
        aa3_to_aa1_d = dict(zip(aa3, aa1))
        aad_d = dict(zip(aa1, range(1, len(aa1)+1)))

        for sfs_data_dir in gen_find_dir(top, filepat):

            print(sfs_data_dir)
            # Get dataset info
            data_info_d = self._sfs_info_formatter(sfs_data_dir, site_type_pos)
            # Get SFS table
            sfs_table = self._sfs_data_reader(
                sfs_data_dir, sfs_format, data_info_d['species'], 
                data_info_d['cod_type'], gene_list, gene_match_func)

            i += 1
            dir_path_d[i] = {
                'sfs_dir': os.path.abspath(sfs_data_dir),
                'sfs_table': sfs_table
            }

            for comp_mut in comp_mutations:
                mutation1 = comp_mut.split('_')[1]
                mutation2 = comp_mut.split('_')[2]
                
                if mutation1 not in sfs_table.index:
                    continue
                elif data_info_d['aa'] in mutation_d_2f:
                    if mutation2 != mutation_d_2f[data_info_d['aa']]:
                        continue
                
                sfs1 = sfs_table.loc[mutation1]
                sfs2 = sfs_table.loc[mutation2]
                assert len(sfs1) == len(sfs2)

                x = list(range(1, len(sfs1)))
                x1 = tests.fd2data(x, sfs1.iloc[:-1], scale)
                x2 = tests.fd2data(x, sfs2.iloc[:-1], scale)
                n1, n2 = sum(sfs1.iloc[:-1]), sum(sfs2.iloc[:-1])

                res = tests.mannwhitneyu_scale(x1, x2, scale)

                site_type_list.append(data_info_d['site_type'])
                cod_type_list.append(data_info_d['cod_type'])
                aa_list.append(data_info_d['aa'])
                species_list.append(data_info_d['species'])
                chr_list.append(data_info_d['chr'])
                filt_code_list.append(data_info_d['filt_code'])
                trim_list.append(data_info_d['trim'])
                geneset_list.append(data_info_d['geneset'])
                comp_str_list.append('_vs_'.join(
                    comp_mut.split('_')[1:]))
                comp_id_list.append(int(comp_mut.split('_')[0]))
                n1_list.append(n1)
                n2_list.append(n2)
                total_n_list.append(n1 + n2)
                mean1_list.append(np.mean(x1))
                mean2_list.append(np.mean(x2))
                mwu_z_list.append(res.z)
                mwu_p_list.append(res.pvalue)
                sfs_id_list.append(i)

        info_df = pd.DataFrame(
            {
                'site_type': site_type_list,
                'cod_type': cod_type_list,
                'aa': aa_list,
                'species': species_list,
                'chr': chr_list,
                'filt_code': filt_code_list,
                'trim': trim_list,
                'geneset': geneset_list,
                'comp_str': comp_str_list,
                'comp': comp_id_list,
                'n1': n1_list,
                'n2': n2_list,
                'total_n': total_n_list,
                'mean1': mean1_list,
                'mean2': mean2_list,
                'mwu_sc100_z': mwu_z_list,
                'mwu_sc100_p': mwu_p_list,
                'sfs_id': sfs_id_list
            }
        )

        def func(x, aad_d):
            try:
                return aad_d[x['aa']]
            except KeyError:
                return -9
        info_df['aad'] = info_df.apply(lambda x: func(x, aad_d), axis=1)

        info_df = info_df\
            .sort_values(by=['chr', 'site_type', 'trim', 'cod_type', 
                             'species', 'aad', 'geneset', 'comp'])\
            .loc[:, ['species', 'site_type', 'chr', 'filt_code', 'trim', 'cod_type', 
                     'aa', 'aad', 'geneset', 'comp', 'comp_str', 'n1', 'n2', 
                     'total_n', 'mean1', 'mean2', 
                     'mwu_sc100_z', 'mwu_sc100_p', 'sfs_id']]
        
        info_df.reset_index(drop=True, inplace=True)

        return info_df, dir_path_d
    
    def _sfs_data_reader(self, sfs_data_dir, sfs_format, species, cod_type,
                         gene_list=[], gene_match_func=None, rep_num=1000):
        if sfs_format == 'gbgSFS':
            # sfs_data_file = os.path.join(
            #     sfs_data_dir, 'estimated_SFS+fixations_of_each_gene_0.txt')

            # gene_id, gbgSFS = parse_genebygene_SFS_file(sfs_data_file)

            # if gene_list:
            #     gene_id, gbgSFS = filter_gbgSFS(
            #         gene_id, gbgSFS, gene_list, gene_match_func)

            # _, fd_boot = genebygeneSFS_to_FDboot(gene_id, gbgSFS, 1, species)
            pass

        elif sfs_format == 'df':
            sfs_data_file = os.path.join(
                sfs_data_dir, 'SFS_bootstrap_data.csv')
            sfs = SFS(
                pd.read_csv(sfs_data_file), species, rep_num, cod_type, 
                description=''
            )
        
        return sfs.base_table

    def __len__(self):
        return len(self._d)
    
    def __iter__(self):
        return self._d.items()
    
    def __getitem__(self, key):
        if key == '*':
            return self.df
        else:
            return self.filter(sfs_id=key)
        # elif isinstance(key, (tuple, list)):
        #     for k in key:
        #         yield self.df.loc[k, :]
