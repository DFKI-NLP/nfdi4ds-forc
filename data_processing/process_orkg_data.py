# from fuzzywuzzy import process
# import matplotlib.pyplot as plt
import ast
import csv
import json
import numpy as np
import pandas as pd

from additional_api_data.api_data import APIData
from orkg_data.Strategy import Strategy
from orkg_data.orkgPyModule import ORKGPyModule
from util import process_abstract_string, remove_non_english, is_english, standardize_doi, cleanhtml_titles, \
    remove_extra_space, drop_non_papers, remove_duplicates, get_orkg_abstract_doi, get_orkg_abstract_title


# import seaborn as sns
# from tqdm import tqdm


# from logs.my_logger import MyLogger

# logger = MyLogger('label_data').logger
# FILE_PATH = os.path.dirname(__file__)


class ORKGData:
    """
    Provides functionality to
        - load metadata for papers from orkg
        - clean orkg data
        - query missing abstracts from crossref and semnatic scholar api + use orkg abstract finder data
        - map research fields from crossref and semantic schoolar to orkg research fields
        - reduce/merge research fields
        - collect and visualize data statistics for the orkg
    """

    def __init__(self, strategy: Strategy) -> None:
        """
        Load data from ORKG API or rdfDump

        Parameters
        ----------
        strategy :
            Strategy to load Data
        """
        self._strategy = ORKGPyModule()
        self.scheduler = []
        self.paper_index_list = []

        self.predicate_id = 'P30'
        self.meta_ids = {
            'doi': 'http://orkg.org/orkg/predicate/P26',
            'author': 'http://orkg.org/orkg/predicate/P27',
            'publication month': 'http://orkg.org/orkg/predicate/P28',
            'publication year': 'http://orkg.org/orkg/predicate/P29',
            'title': 'http://www.w3.org/2000/01/rdf-schema#label',
            'publisher': 'http://orkg.org/orkg/predicate/HAS_VENUE',
            'url': 'http://orkg.org/orkg/predicate/url'
        }

        self.df = pd.DataFrame(columns=['abstract', 'author', 'doi', 'url', 'publication month', 'publication year',
                                        'title', 'paper_id', 'publisher', 'crossref_field', 'semantic_field', 'label'])

        self.data_stats = {'num_papers': 0, 'num_dois': [], 'num_publisher': [], 'num_science_labels': [],
                           'num_urls': []}

    def load_label_data(self) -> pd.DataFrame:
        """
        Initializes dataframe with orkg data.

        Returns dataframe with raw ORKG data
        -------
        """
        predicate_statements = self._strategy.get_statement_by_predicate(self.predicate_id)
        self.df['label'] = predicate_statements['label']
        self.df['paper_id'] = predicate_statements['paper']

        subject_statements = self._strategy.get_statement_by_subject(predicate_statements['paper'], self.meta_ids)
        for column_name, values in subject_statements.items():
            if column_name == 'abstract' or column_name == 'title':
                values = [process_abstract_string(value) for value in values]
            self.df[column_name] = values

        if 'paper_id' in self.df:
            del self.df['paper_id']

        return self.df

    def clean_orkg_data(self) -> pd.DataFrame:
        """
        Cleans orkg raw data in the following steps:
        1. Removes non-papers (papers with no adequate title and no other information)
        2. Removes extra space from titles
        3. Cleans html and other code remnants from titles
        4. Standardizes doi (no "https://doi.org" prefix)
        5. Removes duplicate papers (according to title) and keeps the one with less NaN cells
        6. Removes non-English papers

        :return: cleaned dataframe
        """
        self.df = drop_non_papers(self.df)
        self.df['title'] = self.df['title'].apply(lambda x: remove_extra_space(x))
        self.df['title'] = self.df['title'].apply(lambda x: cleanhtml_titles(x))
        self.df['doi'] = self.df['doi'].apply(lambda x: standardize_doi(x))
        self.df = remove_duplicates(self.df)
        self.df = remove_non_english(self.df)

        return self.df

    def get_abstracts_from_apis(self) -> pd.DataFrame:
        """
        Get abstracts from crossref and semantic scholar using the APIData class
        :return: dataframe with added abstracts
        """
        self.df = APIData(self.df)
        self.df['crossref_field'] = [self.df.get_crossref_data(row['doi'], index)
                                     for index, row in self.df.iterrows()]
        self.df['abstract'] = [ab['abstract'] if ab != {} else {} for ab in self.df['crossref_field']]

        self.df['semantic_field'] = [self.df.get_semantic_scholar_data(row['doi'], index)
                                     for index, row in self.df.iterrows()]

        # make all non-existent abstract cells NaN
        self.df.loc[self.df['abstract'] == '{}', 'abstract'] = np.NaN

        # make all rows of semantic field a dict
        self.df['semantic_field'] = self.df['semantic_field'].apply(lambda x: ast.literal_eval(x))

        # iterate and add abstracts if they exist in semantic scholar data
        for index, row in self.df.iterrows():
            sem_field = row['semantic_field']

        if pd.isnull(row['abstract']):
            if bool(sem_field):
                self.df.at[index, 'abstract'] = sem_field['abstract']

        return self.df

    def get_abstracts_from_orkg(self) -> pd.DataFrame:
        """
        Gets additional abstracts from the data provided by ORKG Abstracts:
        https://gitlab.com/TIBHannover/orkg/orkg-abstracts

        :return: dataframe with added abstracts from ORKG
        """

        orkg_df = pd.read_csv('data_processing/data/orkg_abstracts/orkg_papers.csv')
        orkg_df['title'] = [str(title).lower() for title in orkg_df['title']]
        self.df['orkg_abstract_doi'] = [get_orkg_abstract_doi(row['doi'], orkg_df)
                                        for index, row in self.df.iterrows()]
        self.df['orkg_abstract_title'] = [get_orkg_abstract_title(row['title'], orkg_df) for index, row in
                                          self.df.iterrows()]

        for index, row in self.df.iterrows():
            abst_doi = row['orkg_abstract_doi']
            abst_title = row['orkg_abstract_title']

            if pd.isnull(row['abstract']):
                if abst_doi != 'no_abstract_found' and is_english(abst_doi):
                    self.df.at[index, 'abstract'] = abst_doi
                elif abst_title != 'no_abstract_found' and is_english(abst_title):
                    self.df.at[index, 'abstract'] = abst_title

        self.df.drop(columns=['orkg_abstract_doi', 'orkg_abstract_title'])

        return self.df

    def convert_science_labels(self) -> pd.DataFrame:
        """
        converts 'Science' labels in orkg data to the appropriate label from crossref and semantic scholar
        according to mapping files

        :return: dataframe converted Science labels
        """
        # load df only with science labels
        science_df = self.df.query('label == "Science"')
        science_df['crossref_field'] = science_df['crossref_field'].apply(lambda x: ast.literal_eval(x))

        # load crossref -> orkg mappings
        crossref_path = 'data_processing/data/mappings/research_field_mapping_crossref_field.json'
        with open(crossref_path, 'r') as infile:
            cross_ref_mappings = json.load(infile)

        for index, row in science_df.iterrows():
            crossref_field = row['crossref_field']
            # if crossref_field is not an empty dict
            if bool(crossref_field):
                if len(crossref_field['crossref_field'][0]) > 0:
                    label = crossref_field['crossref_field'][0][0]

                    if label in cross_ref_mappings.keys():
                        self.df.at[index, 'label'] = cross_ref_mappings[label]

        science_df = df.query('label == "Science"')
        science_df['semantic_field'] = science_df['semantic_field'].apply(lambda x: ast.literal_eval(x))

        semanticschol_path = 'data_processing/data/mappings/research_field_mapping_semantic_field.json'
        with open(semanticschol_path, 'r') as infile:
            semanticschol_mappings = json.load(infile)

        for index, row in science_df.iterrows():
            semantic_field = row['semantic_field']
            if bool(semantic_field):
                if semantic_field['semantic_field'] is not None:
                    if len(semantic_field['semantic_field']) > 0:
                        label = semantic_field['semantic_field'][0]

                        if label in semanticschol_mappings.keys():
                            self.df.at[index, 'label'] = semanticschol_mappings[label]

        return self.df

    def reduce_rf(self) -> pd.DataFrame:
        """
        Removes labels (research fields) that belong to the Arts & Humanities field +
        Reduces labels from about 300 to about 50.

        :return: dataframe reduced to 51 labels
        """
        # remove arts&humanities fields
        with open('data_processing/data/mappings/arts_humanities_field.csv', newline='') as f:
            reader = csv.reader(f)
            arts_humanities = list(reader)
        arts_humanities = [item for sublist in arts_humanities for item in sublist]

        self.df = self.df[~self.df['label'].isin(arts_humanities)]

        # reduce remaining research fields
        path = 'data_processing/data/mappings/rf_reduction.json'
        with open(path, 'r') as infile:
            mappings_reduction = json.load(infile)

        for index, row in self.df.iterrows():
            if row['label'] in mappings_reduction.keys():
                self.df.at[index, 'label'] = mappings_reduction[row['label']]

        return self.df

    @property
    def strategy(self) -> Strategy:
        """Load Strategy for ORKG Data"""
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy


def orkg_data_pipeline():
    """
    A function that applies the ORKG data pipeline and saves the result in
    "data_processing/data/orkg_processed_data.csv"
    :return:
    """
    orkg_data = ORKGData(ORKGPyModule())
    orkg_df = orkg_data.load_label_data()
    orkg_df = orkg_data.clean_orkg_data()
    orkg_df = orkg_data.get_abstracts_from_apis()
    orkg_df = orkg_data.get_abstracts_from_orkg()
    orkg_df = orkg_data.convert_science_labels()
    orkg_df = orkg_data.reduce_rf()
    orkg_df.to_csv('data_processing/data/orkg_processed_data.csv', index=False)


def remove_doi_dups(data_df):
    """
    removes rows of df where the doi is duplicated (keeps first one) and saves the data into a csv
    :param data_df:
    :return: -
    """
    data_df = data_df[(~data_df['doi'].duplicated()) | data_df['doi'].isna()]
    data_df.to_csv('data_processing/data/orkg_data_science_conversion_no_dups.csv', index=False)


def get_abstracts_from_orkg(df):
    """
    Gets additional abstracts from the data provided by ORKG Abstracts:
    https://gitlab.com/TIBHannover/orkg/orkg-abstracts
    """

    orkg_df = pd.read_csv('data_processing/data/orkg_abstracts/orkg_papers.csv')
    orkg_df['title'] = [str(title).lower() for title in orkg_df['title']]
    df['orkg_abstract_doi'] = [get_orkg_abstract_doi(row['doi'], orkg_df)
                               for index, row in df.iterrows()]
    df['orkg_abstract_title'] = [get_orkg_abstract_title(row['title'], orkg_df) for index, row in
                                 df.iterrows()]

    for index, row in df.iterrows():
        abst_doi = row['orkg_abstract_doi']
        abst_title = row['orkg_abstract_title']

        if pd.isnull(row['abstract']):
            if abst_doi != 'no_abstract_found' and is_english(abst_doi):
                df.at[index, 'abstract'] = abst_doi
            elif abst_title != 'no_abstract_found' and is_english(abst_title):
                df.at[index, 'abstract'] = abst_title

    df.drop(columns=['orkg_abstract_doi', 'orkg_abstract_title'])

    return df


if __name__ == '__main__':
    df = pd.read_csv('data_processing/data/orkg_data_processed_no_eng.csv')
    df = get_abstracts_from_orkg(df)
    df.to_csv('data_processing/data/orkg_data_processed_20221124.csv', index=False)


