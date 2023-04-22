import pandas as pd
import json
import os
from typing import Dict

from data_processing.arxiv_data_reduction import ArxivDataReduction
from data_processing.orkg_data import ORKGData

FILE_PATH = os.path.dirname(__file__)


class ArxivData:
    """
    A class for processing arXiv data, taken from https://www.kaggle.com/datasets/Cornell-University/arxiv.
    The data was downloaded on 25.11.22.

    Pipeline:
    1. Read arXiv data and processed ORKG data.
    2. Add missing abstracts to ORKG data.
    3. Drop duplicates from arXiv that already exist in ORKG (based on doi)
    4. Use ArxivDataReduction class to:
        - Get single-label arXiv data.
        - Reduce data based on distribution to overall threshold_instances.
    5. Maps arXiv labels to ORKG research fields taxonomy labels.

    This pipeline produces a processed DataFrame of single-label arXiv data (consisting of a  desired number of
    data points) with ORKG labels.
    """

    def __init__(self,
                 arxiv_data_path="data_processing/data/arxiv_data/arxiv-metadata-oai-snapshot.json",
                 orkg_data_df_path="",
                 threshold_instances=50000):
        self.arxiv_data_path = arxiv_data_path
        self.arxiv_df = pd.read_json(self.arxiv_data_path, lines=True)
        self.orkg_df = pd.read_csv(orkg_data_df_path)
        self.threshold_instances = threshold_instances
        self.mapping_arxiv_orkg = self._load_mapping('data/mappings/arxiv_to_orkg_fields.json')
        self.arxiv_labels = list(self.mapping_arxiv_orkg.keys())
        self.arxiv_distribution = {}
        self.arxiv_distribution_reduced = {}
        self.reduced_data = ArxivDataReduction(self.arxiv_df, self.arxiv_labels,
                                               self.arxiv_distribution, self.arxiv_distribution_reduced)

        # read orkg data from csv if path is given, if not, run ORKGData class
        if orkg_data_df_path != "":
            self.orkg_df = pd.read_csv(orkg_data_df_path)
        else:
            orkg_data = ORKGData()
            self.orkg_df = orkg_data.run()

    def run(self) -> (pd.DataFrame, pd.DataFrame):
        """
        A function that runs the whole pipeline of the ArxivData Class.
        It returns the ORKG data with added abstracts and the single-label arXiv data
        (consisting of a desired number of data points)
        """
        self.orkg_df, self.arxiv_df = self._drop_orkg_dups()
        self.orkg_df = self._add_abstracts_orkg(self.arxiv_df)
        reduced_arxiv_data = self._get_reduced_data(self.threshold_instances)
        reduced_arxiv_data = self._map_arxiv_to_orkg(reduced_arxiv_data)

        return self.orkg_df, reduced_arxiv_data

    def _drop_orkg_dups(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Removes papers from the Arxiv imported data that already exist in the ORKG data
        + updates the ORKG data with additional abstracts from Arxiv
        :return: 1. ORKG data with added abstracts, 2. Arxiv data with removed duplicates
        """
        self.arxiv_df['in_orkg_data'] = [row['doi'] in self.orkg_df['doi'].values
                                         for index, row in self.arxiv_df.iterrows()]
        # Dataframe with papers that exist in both ORKG and Arxiv
        arxiv_orkg_data = self.arxiv_df.query('in_orkg_data==True')
        self.orkg_df = self.add_abstracts_orkg(arxiv_orkg_data)
        self.arxiv_df = self.arxiv_df.query('in_orkg_data==False')

        return self.orkg_df, self.arxiv_df

    def _add_abstracts_orkg(self, arxiv_orkg_data: pd.DataFrame) -> pd.DataFrame:
        """
        Adds abstracts to papers that exist both in Arxiv and ORKG and don't have an abstract from
        Crossref/Semantic Scholar
        :return: ORKG data with added abstracts
        """
        for index, row in arxiv_orkg_data.iterrows():
            if pd.isna(self.orkg_df[self.orkg_df['doi'] == row['doi']]['abstract'].values[0]):
                idx = self.orkg_df[self.orkg_df['doi'] == row['doi']]['abstract'].index
                self.orkg_df.at[idx[0], 'abstract'] = row['abstract']

        return self.orkg_df

    def _get_reduced_data(self, threshold_instances: int) -> pd.DataFrame:
        """
        A function that returns a DataFrame of single-label arXiv data (consisting of a desired number of data points)
        Using the ArxivDataReduction class.
        :param threshold_instances: the desired number of data points
        """
        return self.reduced_data.get_reduced_data(threshold_instances)

    @staticmethod
    def _load_mapping(filename: str) -> Dict[str, str]:
        """ load mapping dict with label mapping between arxiv and orkg labels """
        json_path = os.path.join(FILE_PATH, filename)
        with open(json_path, 'r') as jsonfile:
            mapping = json.load(jsonfile)
        return mapping

    def _map_arxiv_to_orkg(self, single_label_arxiv_reduced: pd.DataFrame) -> pd.DataFrame:
        """
        A function that maps the arXiv categories taxonomy to the ORKG research field taxonomy labels
        and saves the newly created dataset (with ORKG labels) as
        'data_processing/data/arxiv_data/arxiv_reduced_orkg_labels.csv'
        :param single_label_arxiv_reduced: the arXiv single-label dataset after reducing its instances
        """
        for index, row in single_label_arxiv_reduced.iterrows():
            single_label_arxiv_reduced.at[index, 'categories'] = self.mapping_arxiv_orkg[row['categories']]

        return single_label_arxiv_reduced


if __name__ == '__main__':
    arxiv = ArxivData()
    orkg_df, arxiv_df = arxiv.run()
    arxiv_df.to_csv('data_processing/data/arxiv_data/arxiv_reduced_orkg_labels.csv')
