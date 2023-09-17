import ast
import csv
import json
import numpy as np
import pandas as pd
from typing import Dict
from orkg_data.Strategy import Strategy
from orkg_data.orkgPyModule import ORKGPyModule
from data_cleaning_utils import process_abstract_string, get_orkg_abstract_doi, get_orkg_abstract_title

from orkg_data.clean_data import ORKGDataCleaner
from orkg_data.science_label_converter import ScienceLabelConverter
from orkg_data.get_abstracts import DataAbstracts



class ORKGData:
    """
    Provides functionality to:
        - Load metadata for papers from ORKG.
        - Clean orkg data using the ORKGDataCleaner class.
        - Query missing abstracts using: Crossref, S2AG, OpenAlex, or ORKG Abstract Finder repo.
        - For papers labelled as 'Science' in ORKG, get the correct label from Crossref/Semantic Scholar.
        - Integrate manual re-labeling of remaining papers tagged as 'Science'.
        - Merge research fields to reduce their number.
    """

    def __init__(self) -> None:
        """
        Load data from ORKG API or rdfDump
        """
        self._strategy = ORKGPyModule()

        # The id of the predicate 'research field' in ORKG.
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

        self.orkg_df = pd.DataFrame(
            columns=['abstract', 'author', 'doi', 'url', 'publication month', 'publication year',
                     'title', 'paper_id', 'publisher', 'crossref_field', 'semantic_field', 'label'])

    def run(self) -> pd.DataFrame:
        """
        Runs the following processing pipeline for ORKG data:
            - Load ORKG data.
            - Clean ORKG data using the ORKGDataCleaner class.
            - Query additional abstracts from APIs using the DataAbstracts class.
            - Convert 'Science' labels to correct labels using the ScienceLabelConverter class.
            - Merge research fields to reduce their number.
        The output is the processed ORKG dataset in the format of a pd.DataFrame.
        """
        self._load_label_data()
        self.orkg_df = ORKGDataCleaner(self.orkg_df).run()
        self.orkg_df = DataAbstracts(self.orkg_df).run()
        self.orkg_df = ScienceLabelConverter(self.orkg_df).run()
        self.orkg_df = self._reduce_rf()

        return self.orkg_df

    def _load_label_data(self) -> None:
        """
        Initializes dataframe with orkg data.

        Returns dataframe with raw ORKG data
        -------
        """
        predicate_statements = self._strategy.get_statement_by_predicate(self.predicate_id)
        self.orkg_df['label'] = predicate_statements['label']
        self.orkg_df['paper_id'] = predicate_statements['paper']

        subject_statements = self._strategy.get_statement_by_subject(predicate_statements['paper'], self.meta_ids)
        for column_name, values in subject_statements.items():
            if column_name == 'abstract' or column_name == 'title':
                values = [process_abstract_string(value) for value in values]
            self.orkg_df[column_name] = values

        if 'paper_id' in self.orkg_df:
            del self.orkg_df['paper_id']

    def _reduce_rf(self) -> pd.DataFrame:
        """
        Re-label Arts and Humanities sub-fields to the higher level class +
        Reduces labels from about 300 to about 50.

        :return: dataframe reduced to 51 labels
        """
        # re-label arts&humanities fields (keeping only the higher level class: Arts and Humanities)
        with open('data_processing/data/mappings/arts_humanities_field.csv', newline='') as f:
            reader = csv.reader(f)
            arts_humanities = list(reader)

        arts_humanities = [item for sublist in arts_humanities for item in sublist]

        self.orkg_df['label'] = ['Arts and Humanities' for item in self.orkg_df['label'] if item.isin(arts_humanities)]

        # reduce remaining research fields
        path = 'data_processing/data/mappings/rf_reduction.json'
        with open(path, 'r') as infile:
            mappings_reduction = json.load(infile)

        for index, row in self.orkg_df.iterrows():
            if row['label'] in mappings_reduction.keys():
                self.orkg_df.at[index, 'label'] = mappings_reduction[row['label']]

        return self.orkg_df

    @property
    def strategy(self) -> Strategy:
        """Load Strategy for ORKG Data"""
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy


def remove_doi_dups(data_df):
    """
    removes rows of df where the doi is duplicated (keeps first one) and saves the data into a csv
    :param data_df:
    :return: -
    """
    data_df = data_df[(~data_df['doi'].duplicated()) | data_df['doi'].isna()]
    data_df.to_csv('data_processing/data/orkg_data_science_conversion_no_dups.csv', index=False)

