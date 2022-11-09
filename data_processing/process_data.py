# from fuzzywuzzy import process
# import matplotlib.pyplot as plt
import ast
import csv
import json
import re
import spacy
import spacy_fastlang
import numpy as np
import pandas as pd

from additional_api_data.api_data import APIData
from orkg_data.Strategy import Strategy
from orkg_data.orkgPyModule import ORKGPyModule
from util import process_abstract_string


# import seaborn as sns
# from tqdm import tqdm


# from logs.my_logger import MyLogger

# logger = MyLogger('label_data').logger
# FILE_PATH = os.path.dirname(__file__)


class ORKGData:
    """
    Provides functionality to
        - load metadata for papers from orkg
        - query missing data from crossref and semnatic scholar api
        - map research fields from crossref and semantic schoolar to orkg research fields
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

    def load_label_data(self) -> None:
        """
        Initializes dataframe with orkg data.

        Returns
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

    @property
    def strategy(self) -> Strategy:
        """Load Strategy for ORKG Data"""
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy


def convert_science_labels(df):
    """
    converts 'Science' labels in orkg data to the appropriate label from crossref and semantic scholar
    according to mapping files
    :param df: dataframe of data from orkg
    :return: dataframe after converting labels
    """
    # load df only with science labels
    science_df = df.query('label == "Science"')
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
                    df.at[index, 'label'] = cross_ref_mappings[label]

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
                        df.at[index, 'label'] = semanticschol_mappings[label]

    return df


def fetch_orkg_data():
    orkg_data = ORKGData(ORKGPyModule())
    orkg_data.load_label_data()
    orkg_data.df.to_csv('data_processing/data/orkg_raw_data.csv', index=False)


def get_abstracts(raw_data_df):
    """
    Get abstracts from crossref and semantic scholar using the APIData class
    :param raw_data_df: raw data fetched from orkg
    :return: df with abstracts
    """
    data_df = raw_data_df
    api_data = APIData(data_df)
    data_df['crossref_field'] = [api_data.get_crossref_data(row['doi'], index)
                                 for index, row in data_df.iterrows()]
    data_df['abstract'] = [ab['abstract'] if ab != {} else {} for ab in data_df['crossref_field']]

    data_df['semantic_field'] = [api_data.get_semantic_scholar_data(row['doi'], index)
                                 for index, row in data_df.iterrows()]

    # make all non-existent abstract cells NaN
    data_df.loc[data_df['abstract'] == '{}', 'abstract'] = np.NaN

    # make all rows of semantic field a dict
    data_df['semantic_field'] = data_df['semantic_field'].apply(lambda x: ast.literal_eval(x))

    # iterate and add abstracts if they exist in semantic scholar data
    for index, row in data_df.iterrows():
        sem_field = row['semantic_field']

    if pd.isnull(row['abstract']):
        if bool(sem_field):
            data_df.at[index, 'abstract'] = sem_field['abstract']

    return data_df


def remove_doi_dups(data_df):
    """
    removes rows of df where the doi is duplicated (keeps first one) and saves the data into a csv
    :param data_df:
    :return: -
    """
    data_df = data_df[(~data_df['doi'].duplicated()) | data_df['doi'].isna()]
    data_df.to_csv('data_processing/data/orkg_data_science_conversion_no_dups.csv', index=False)


def reduce_rf(data_df):
    """
    Removes labels (research fields) that belong to the Arts & Humanities field +
    Reduces labels from about 300 to about 50.
    :param data_df: a dataframe consisting of the elements fetched from ORKG
    :return: data_df with no Art&Humanities labels + reduced labels
    """
    # remove arts&humanities fields
    with open('data_processing/data/mappings/arts_humanities_field.csv', newline='') as f:
        reader = csv.reader(f)
        arts_humanities = list(reader)
    arts_humanities = [item for sublist in arts_humanities for item in sublist]

    data_df = data_df[~data_df['label'].isin(arts_humanities)]

    # reduce remaining research fields
    path = 'data_processing/data/mappings/rf_reduction.json'
    with open(path, 'r') as infile:
        mappings_reduction = json.load(infile)

    for index, row in data_df.iterrows():
        if row['label'] in mappings_reduction.keys():
            data_df.at[index, 'label'] = mappings_reduction[row['label']]

    return data_df


def drop_non_papers(df):
    """

    :param df: dataframe with fetched data
    :return: the same dataframe with rows that do not contain actual papers dropped
    """
    df.drop(df.index[((df['title'].str.len() <= 20) | pd.isnull(df['title'])) & (pd.isnull(df['url'])) &
                     (pd.isnull(df['doi'])) & (pd.isnull(df['abstract'])) & (pd.isnull(df['author']))],
            inplace=True)
    df = df.query('title != "deleted"')
    df = df.query('title != "Deleted"')

    return df


def remove_extra_space(text):
    if isinstance(text, str):
        text = text.replace(u'\xa0', u' ')
        text = text.replace(u'\u2002', u' ')
        text = re.sub(' +', ' ', text)
        text = text.strip()
        text = text.lower()
    return text


def cleanhtml_titles(raw_html):
    if isinstance(raw_html, str):
        CLEANR = re.compile('<.*?>')
        cleantext = re.sub(CLEANR, '', raw_html)
        return cleantext
    return raw_html


def standardize_doi(doi):
    if type(doi) == 'str':
        if doi.startswith('https://doi.org/'):
            doi = doi[16:]

    return doi


def is_english(text):
    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe("language_detector")
    doc = nlp(text)
    return doc._.language == 'en'


def remove_duplicates(df):
    """
    A function that removes duplicat papers according to title, and keeps the one with the least NaN elements in it.
    :param df: dataframe of orkg data
    :return: the same dataframe with dropped duplicates
    """
    df['crossref_field'] = df['crossref_field'].apply(lambda x: np.nan if x == '{}' else x)
    df['semantic_field'] = df['semantic_field'].apply(lambda x: np.nan if x == '{}' else x)

    df['nan_count'] = [df.loc[index].isna().sum().sum() for index, row in df.iterrows()]
    df = df.sort_values('nan_count', ascending=True).drop_duplicates('title', keep='first').sort_index()
    df = df.drop(columns=['nan_count'])

    return df


if __name__ == '__main__':
    df = pd.read_csv('data_processing/data/orkg_data_reduced_fields.csv')
    df = drop_non_papers(df)
    df['title'] = df['title'].apply(lambda x: remove_extra_space(x))
    df['title'] = df['title'].apply(lambda x: cleanhtml_titles(x))
    df['doi'] = df['doi'].apply(lambda x: standardize_doi(x))

    df = remove_duplicates(df)
    df.to_csv('data_processing/data/orkg_data_processed.csv', index=False)
