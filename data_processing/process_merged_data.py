import pandas as pd
import matplotlib.pyplot as plt
from data_cleaning_utils import process_abstract, remove_non_english

from process_arxiv_data import ArxivData


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
        - remove_non_english
        - visualize_nan_columns
        Saves the merged dataset to data_processing/data/merged_data.csv.
        """
        merged_df = self._merge_datasets()
        print("Merged dataset created...")
        merged_df = self._process_abstracts(merged_df)
        print("Preprocessed abstracts...")
        merged_df = remove_non_english(merged_df)
        print("Removed non-English papers...")
        merged_df.to_csv('data_processing/data/merged_data.csv')
        print("Merged dataset saved to data_processing/data/merged_data.csv")

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


if __name__ == '__main__':
    merged_data = MergedData()
    merged_data.run()
