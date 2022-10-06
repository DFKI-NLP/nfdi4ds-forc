import concurrent.futures
import json
import os
import re
import time
from typing import Dict

# from fuzzywuzzy import process
# import matplotlib.pyplot as plt
import pandas as pd
# import seaborn as sns
# from tqdm import tqdm

# from data_processing.api_data.api_data import APIData
from orkg_data.Strategy import Strategy
from orkg_data.orkgPyModule import ORKGPyModule


from util import recursive_items, process_abstract_string
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
    orkg_data.load_label_data()
    # create csv for labeled orkgdata
    orkg_data.df.to_csv('data_processing/data/orkg_raw_data.csv', index=False)
