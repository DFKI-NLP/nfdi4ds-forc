import pandas as pd
import json
import os
from typing import List, Dict, Tuple

FILE_PATH = os.path.dirname(__file__)


class ArxivData:

    def __init__(self,
                 arxiv_data_path="data_processing/data/arxiv_data/arxiv-metadata-oai-snapshot.json",
                 orkg_data_df_path="data_processing/data/orkg_data_processed_20221124.csv"):
        self.arxiv_data_path = arxiv_data_path
        self.arxiv_df = pd.read_json(self.arxiv_data_path, lines=True)
        self.orkg_df = pd.read_csv(orkg_data_df_path)
        self.mapping_arxiv_orkg = self._load_mapping('data/mappings/arxiv_to_orkg_fields.json')
        self.arxiv_labels = list(self.mapping_arxiv_orkg.keys())
        self.arxiv_distribution = {}
        self.arxiv_distribution_reduced = {}

    def drop_orkg_dups(self):
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

    def add_abstracts_orkg(self, arxiv_orkg_data):
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

    def get_single_label_instance(self, arxiv_df) -> pd.DataFrame:
        """
        A function that filters out the single-label instances from the Arxiv data,
        and returns a new dataframe with only those instances
        :param arxiv_df: The full arxive data
        :return: The Arxive data with only single-label instances
        """

        arxiv_df['multi_label'] = [' ' in row['categories'] for index, row in arxiv_df.iterrows()]
        single_label_arxiv = arxiv_df.query('multi_label == False')

        return single_label_arxiv

    def get_single_label_distribution(self, single_label_arxiv) -> Dict:
        """

        :param single_label_arxiv:
        :return:
        """
        single_labels_stats = single_label_arxiv.groupby("categories")["categories"].count()
        return single_labels_stats.to_dict()

    def get_reduced_data(self, single_label_arxiv, threshold_instances) -> pd.DataFrame:
        """
    
        :param single_label_arxiv: the full Arxiv dataset with no reduction of fields
        :param threshold_instances: desired number of instances overall (for all labels together)
        :return: 
        """
        self.arxiv_distribution = self.get_single_label_distribution(single_label_arxiv)
        df_length = sum(self.arxiv_distribution.values())

        self.arxiv_distribution_reduced = {label: int(count / df_length * threshold_instances) for label, count in
                                      self.arxiv_distribution.items()}

        single_label_arxiv_reduced = pd.DataFrame()

        for key, value in self.arxiv_distribution_reduced.items():
            single_label_arxiv_reduced = pd.concat([
                single_label_arxiv_reduced,
                self.get_reduced_rows(key, value, single_label_arxiv)
            ])

        return single_label_arxiv_reduced

    def get_reduced_rows(self, label, n_instances, arxiv_df):
        return arxiv_df.query("categories == '{}'".format(label)).sample(n=n_instances)

    @staticmethod
    def _load_mapping(filename: str) -> Dict[str, str]:
        """ load mapping dict with label mapping between arxiv and orkg labels """
        json_path = os.path.join(FILE_PATH, filename)
        with open(json_path, 'r') as jsonfile:
            mapping = json.load(jsonfile)
        return mapping

    def map_arxiv_to_orkg(self, single_label_arxiv_reduced):
        """

        :param single_label_arxiv_reduced:
        :return:
        """
        for index, row in single_label_arxiv_reduced.iterrows():
            single_label_arxiv_reduced.at[index, 'categories'] = self.mapping_arxiv_orkg[row['categories']]

        single_label_arxiv_reduced.to_csv('data_processing/data/arxiv_data/arxiv_reduced_orkg_labels.csv')


if __name__ == '__main__':
    single_label_arxiv = pd.read_csv('data_processing/data/arxiv_single_label_data.csv')
    arxiv = ArxivData()
    reduced_arxiv_data = arxiv.get_reduced_data(single_label_arxiv, 50000)
    arxiv.map_arxiv_to_orkg(reduced_arxiv_data)

