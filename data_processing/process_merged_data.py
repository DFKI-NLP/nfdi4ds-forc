import pandas as pd
import matplotlib.pyplot as plt


class MergedData:

    def __init__(self, orkg_data_path='data_processing/data/orkg_data_processed_20221125.csv',
                 arxiv_data_path='data_processing/data/arxiv_data/arxiv_reduced_orkg_labels.csv'):
        self.orkg_df = pd.read_csv(orkg_data_path)
        self.arxiv_df = pd.read_csv(arxiv_data_path)

    def merge_datasets(self) -> pd.DataFrame:
        self.arxiv_df = self.arxiv_df.rename(columns=
                                             {"authors": "author",
                                              "categories": "label",
                                              "id": "arxiv_id",
                                              "submitter": "arxiv_submitter",
                                              "journal-ref": "arxiv_journal-ref",
                                              "report-no": "arxiv_report-no",
                                              "license": "arxiv_license",
                                              "versions": "arxiv_versions",
                                              "update_date": "arxiv_update_date"})
        self.arxiv_df = self.arxiv_df.drop(columns=["Unnamed: 0", "in_orkg_data", "multi_label"])
        self.orkg_df = self.orkg_df.drop(columns=["orkg_abstract_doi", "orkg_abstract_title"])

        self.arxiv_df['source'] = "arxiv"
        self.orkg_df['source'] = "orkg"

        merged_df = pd.concat([self.orkg_df, self.arxiv_df])

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
    data = MergedData()
    merged_df = data.merge_datasets()
    data.visualize_nan_columns(merged_df)

    merged_df.to_csv('data_processing/data/merged_data.csv')
