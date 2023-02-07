import pandas as pd
import matplotlib.pyplot as plt
from data_cleaning_utils import process_abstract


class MergedData:

    def __init__(self, orkg_data_path='data_processing/data/orkg_data_processed.csv',
                 arxiv_data_path='data_processing/data/arxiv_data/arxiv_reduced_orkg_labels.csv'):
        self.orkg_df = pd.read_csv(orkg_data_path)
        self.arxiv_df = pd.read_csv(arxiv_data_path)

    def merge_datasets(self) -> pd.DataFrame:
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

    def process_abstracts(self, merged_df):
        """
        processes the abstract texts by removing code elements
        :param merged_df
        :return: the same dataset with processed abstracts
        """
        merged_df['abstract'] = merged_df['abstract'].apply(lambda x: process_abstract(x) if not pd.isna(x)
                                                            else x)
        return merged_df

    def visualize_nan_columns(self, merged_df):
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
    data = MergedData(orkg_data_path='data_processing/data/orkg_data_processed_rg.csv')
    merged_df = data.merge_datasets()
    merged_df = data.process_abstracts(merged_df)
    data.visualize_nan_columns(merged_df)

    merged_df.to_csv('data_processing/data/merged_data.csv')
    
