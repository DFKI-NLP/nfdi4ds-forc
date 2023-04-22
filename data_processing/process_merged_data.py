import pandas as pd
import matplotlib.pyplot as plt
from data_cleaning_utils import process_abstract

from arxiv_data import ArxivData


class MergedData:
    """
    This class merges the arxiv and orkg datasets and processes the abstracts. It can also visualize the number of NaN
    elements per column in the merged dataset.

    It saves the merged dataset to data_processing/data/merged_data.csv.
    """

    def __init__(self):
        self.arxiv_data = ArxivData()
        self.orkg_df, self.arxiv_df = arxiv_data.run()

    def run(self) -> None:
        """
        Runs the following methods:
        - merge_datasets
        - process_abstracts
        - visualize_nan_columns
        Saves the merged dataset to data_processing/data/merged_data.csv.
        """
        merged_df = self._merge_datasets()
        merged_df = self._process_abstracts(merged_df)
        merged_df.to_csv('data_processing/data/merged_data.csv')
        self._visualize_nan_columns(merged_df)

    def _merge_datasets(self) -> pd.DataFrame:
        """
        Merges the arxiv and orkg datasets.
        """
        self.arxiv_df = self.arxiv_df.rename(columns=
                                             {"authors": "author",
                                              "categories": "label",
                                              "id": "arxiv_id",
                                              "submitter": "arxiv_submitter",
                                              "journal-ref": "publisher",
                                              "report-no": "arxiv_report-no",
                                              "license": "arxiv_license",
                                              "versions": "arxiv_versions",
                                              "update_date": "arxiv_update_date"})
        self.arxiv_df = self.arxiv_df.drop(columns=["Unnamed: 0", "in_orkg_data", "multi_label"])

        self.arxiv_df['source'] = "arxiv"
        self.orkg_df['source'] = "orkg"

        merged_df = pd.concat([self.orkg_df, self.arxiv_df])
        return merged_df

    def _process_abstracts(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        processes the abstract texts by removing code elements
        :param merged_df
        :return: the same dataset with processed abstracts
        """
        merged_df['abstract'] = merged_df['abstract'].apply(lambda x: process_abstract(x) if not pd.isna(x) else x)
        return merged_df

    def _visualize_nan_columns(self, merged_df: pd.DataFrame):
        """
        Visualizes the number of NaN elements per column in merged_df.
        """
        columns = merged_df.columns.to_list()
        nan_info = {}
        for column in columns:
            nan_info[column] = merged_df[column].isna().sum()

        plt.figure(figsize=(20, 7))
        plt.bar(range(len(nan_info)), list(nan_info.values()), align='center', color=(0.2, 0.4, 0.6, 0.6))
        plt.xticks(range(len(nan_info)), list(nan_info.keys()))
        plt.xticks(rotation=35)
        plt.title('Number of NaN elements per column in Dataframe '
                  '(overall number of papers is {})'.format(len(merged_df)))
        plt.show()


if __name__ == '__main__':
    merged_data = MergedData()
    merged_data.run()
