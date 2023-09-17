import pandas as pd
import json
import os
from typing import Dict, List

FILE_PATH = os.path.dirname(__file__)


class ArxivDataReduction:
    """
    A class that reduces the amount of overall instances in the data to a desired number (threshold_instances).
    The purpose of this class is to:
    1. Get a single-label arXiv dataset
    2. Get a new dataset with a desired number of instances (threshold_instances) with the same distribution of labels

    This class is used in the ArxivData class.
    """

    def __init__(self, arxiv_df: pd.DataFrame, arxiv_labels: List[str], arxiv_distribution: Dict,
                 arxiv_distribution_reduced: Dict):
        self.arxiv_df = arxiv_df
        self.arxiv_labels = arxiv_labels
        self.arxiv_distribution = arxiv_distribution
        self.arxiv_distribution_reduced = arxiv_distribution_reduced

    def get_reduced_data(self, threshold_instances: int) -> pd.DataFrame:
        """
        A function that reduces the amount of overall instances in the data to a desired number (threshold_instances).
        The reduction keeps the original distribution of labels.
        :param threshold_instances: desired number of instances overall (for all labels together)
        :return: a new pandas dataframe with a length of threshold_instances, with labels distribution kept
        as the input single_label_arxiv
        """
        single_label_arxiv = self._get_single_label_instance()
        self.arxiv_distribution = self._get_single_label_distribution(single_label_arxiv)
        df_length = sum(self.arxiv_distribution.values())

        # define the new distribution of labels based on threshold_instances
        self.arxiv_distribution_reduced = {label: int(count / df_length * threshold_instances) for label, count in
                                           self.arxiv_distribution.items()}
        # initialize a new dataframe
        single_label_arxiv_reduced = pd.DataFrame()

        # get random reduced rows for each label based on its new distribution
        for key, value in self.arxiv_distribution_reduced.items():
            single_label_arxiv_reduced = pd.concat([
                single_label_arxiv_reduced,
                self._get_reduced_rows(key, value, single_label_arxiv)
            ])

        return single_label_arxiv_reduced

    def _get_single_label_instance(self) -> pd.DataFrame:
        """
        A function that filters out the single-label instances from the Arxiv data,
        and returns a new dataframe with only those instances
        :param arxiv_df: The full arxive data
        :return: The Arxive data with only single-label instances
        """

        self.arxiv_df['multi_label'] = [' ' in row['categories'] for index, row in arxiv_df.iterrows()]
        single_label_arxiv = arxiv_df.query('multi_label == False')

        return single_label_arxiv

    def _get_single_label_distribution(self, single_label_arxiv: pd.DataFrame) -> Dict:
        """
        A function that calculated the distribution of labels from the single-label instances of arXiv.
        :param single_label_arxiv: the arXiv data filtered to instances with a sing-label.
        :return: a dictionary consisting of 'label: number of occurences'.
        """
        single_labels_stats = single_label_arxiv.groupby("categories")["categories"].count()
        return single_labels_stats.to_dict()

    def _get_reduced_rows(self, label: str, n_instances: int, arxiv_df: pd.DataFrame) -> pd.DataFrame:
        """
        A function that gets a certain label and the number of desired instances,
        and returns n_instances of randomly selected rows with the input label from arxiv_df.
        :param label: the desired label for selection of rows
        :param n_instances: the desired number of rows to randomly select
        :param arxiv_df: the arXiv dataset of single-labels
        :return: n_instances of randomly selected rows with the input label from arxiv_df
        """
        return arxiv_df.query("categories == '{}'".format(label)).sample(n=n_instances)
