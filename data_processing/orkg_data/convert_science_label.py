import pandas as pd
import json
import ast


class ScienceLabelConverter:
    """
    Converts 'Science' labels in orkg data to the appropriate label from crossref and semantic scholar.
    Note that this class includes a manual re-labeling step, which is done by exporting the science labels to a .csv
    file and then re-importing the relabeled file in .xlsx format; in which new labels are added.
    """

    def __init__(self, orkg_df):
        self.orkg_df = orkg_df

    def run(self) -> pd.DataFrame:
        """
        runs the conversion process of science labels
        :return: dataframe with converted science labels
        """
        self.orkg_df = self._convert_science_labels()
        self.export_science_labels(self.orkg_df)
        self.orkg_df = self.relabel_science_manual(self.orkg_df)
        return self.orkg_df

    def _convert_science_labels(self) -> pd.DataFrame:
        """
        converts 'Science' labels in orkg data to the appropriate label from crossref and semantic scholar
        according to mapping files

        :return: dataframe converted Science labels
        """
        # load df only with science labels
        science_df = self.orkg_df.query('label == "Science"')
        science_df['crossref_field'] = science_df['crossref_field'].apply(lambda x: ast.literal_eval(x))

        # load crossref -> orkg mappings
        crossref_path = 'data_processing/data/mappings/research_field_mapping_crossref_field.json'
        with open(crossref_path, 'r') as infile:
            cross_ref_mappings = json.load(infile)

        for index, row in science_df.iterrows():
            crossref_field = row['crossref_field']
            # if crossref_field is not an empty dict
            if bool(crossref_field):
                if len(crossref_field['crossref_field'][0]) > 0:
                    label = crossref_field['crossref_field'][0][0]

                    if label in cross_ref_mappings.keys():
                        self.orkg_df.at[index, 'label'] = cross_ref_mappings[label]

        science_df = df.query('label == "Science"')
        science_df['semantic_field'] = science_df['semantic_field'].apply(lambda x: ast.literal_eval(x))

        semanticschol_path = 'data_processing/data/mappings/research_field_mapping_semantic_field.json'
        with open(semanticschol_path, 'r') as infile:
            semanticschol_mappings = json.load(infile)

        for index, row in science_df.iterrows():
            semantic_field = row['semantic_field']
            if bool(semantic_field):
                if semantic_field['semantic_field'] is not None:
                    if len(semantic_field['semantic_field']) > 0:
                        label = semantic_field['semantic_field'][0]

                        if label in semanticschol_mappings.keys():
                            self.orkg_df.at[index, 'label'] = semanticschol_mappings[label]

        return self.orkg_df

    def _export_science_labels(self, export_path="data_processing/data/science_labels.csv") -> None:
        """
        Exports science labels to .csv in preparation for manual re-labelling.
        :parameter orkg_df: dataframe with science labels
        :parameter export_path: path to export the science labels to
        """
        science_df = self.orkg_df.query('label == "Science"')
        science_df.to_csv(export_path, index=False)

    def _relabel_science_manual(self, orkg_df,
                               csv_path="data_processing/data/merged_data_relabeling_science.csv") \
            -> pd.DataFrame:
        """
        relabels the remaining 'Science' labels manually by:
        1. Importing the relabeled file in .xlsx format; in which nee labels are added in the 'new_label' column.
        2. Merging the relabeled dataframe with the original dataframe.
        :return:

        """
        science_relabeled_df = pd.csv(csv_path)
        orkg_df['label'] = [self._get_new_label(row, science_relabeled_df) for index, row in
                            orkg_df.iterrows()]

        return orkg_df

    def _get_new_label(self, row, science_relabeled_df) -> str:
        """
        get the new label of a row originally tagged as 'Science'
        :param row: row in the original dataframe
        :param science_relabeled_df: dataframe with the manually relabeled science labels
        :return: new label
        """
        title = row['title']
        label = row['label']
        if title in science_relabeled_df['title'].values:
            label = science_relabeled_df[science_relabeled_df['title'] == title]['new_label']
            return label.values[0]
        return label
