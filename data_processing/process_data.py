# from fuzzywuzzy import process
# import matplotlib.pyplot as plt
import ast

import numpy as np
import pandas as pd

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


if __name__ == '__main__':
    orkg_data = ORKGData(ORKGPyModule())
    # orkg_data.load_label_data()
    # create csv for labeled orkgdata
    # orkg_data.df.to_csv('data_processing/data/orkg_raw_data.csv', index=False)
    # raw_data_df = pd.read_csv('data_processing/data/orkg_raw_data.csv')
    # api_data = APIData(raw_data_df)

    # raw_data_df['crossref_field'] = [api_data.get_crossref_data(row['doi'], index)
    #                           for index, row in raw_data_df.iterrows()]

    # raw_data_df['abstract'] = [ab['abstract'] if ab != {} else {} for ab in raw_data_df['crossref_field']]

    data_df = pd.read_csv('data_processing/data/orkg_data_semschol_data.csv')

    # api_data = APIData(crossref_data_df)

    # crossref_data_df['semantic_field'] = [api_data.get_semantic_scholar_data(row['doi'], index)
    #                               for index, row in crossref_data_df.iterrows()]

    # crossref_data_df.to_csv('data_processing/data/orkg_data_semschol_abstracts.csv', index=False)

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

    data_df.to_csv('data_processing/data/orkg_data_semschol_abstracts.csv', index=False)
