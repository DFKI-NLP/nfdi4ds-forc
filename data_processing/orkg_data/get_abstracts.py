import pandas as pd
import numpy as np

from additional_api_data.api_data import APIData


class DataAbstracts:
    def __init__(self, orkg_df: pd.DataFrame, email: str):
        self.orkg_df = orkg_df
        self.email = email

    def run(self) -> pd.DataFrame:
        """
        Runs the abstracts class
        :return: dataframe with added abstracts
        """
        self.orkg_df = self._get_abstracts_from_apis()
        self.orkg_df = self._get_abstracts_from_orkg()
        return self.orkg_df

    def _get_abstracts_from_apis(self) -> pd.DataFrame:
        """
        Get abstracts from crossref and semantic scholar (s2ag) using the APIData class
        :return: dataframe with added abstracts
        """
        api_data = APIData(self.orkg_df)
        self.orkg_df['crossref_field'] = [api_data.get_crossref_data(row['doi'], index)
                                          for index, row in self.orkg_df.iterrows()]
        self.orkg_df['abstract'] = [ab['abstract'] if ab != {} else {} for ab in self.orkg_df['crossref_field']]

        self.orkg_df['semantic_field'] = [api_data.get_s2ag_data(row['doi'], index)
                                          for index, row in self.orkg_df.iterrows()]

        # make all non-existent abstract cells NaN
        self.orkg_df.loc[self.orkg_df['abstract'] == '{}', 'abstract'] = np.NaN

        # make all rows of semantic field a dict
        self.orkg_df['semantic_field'] = self.orkg_df['semantic_field'].apply(lambda x: ast.literal_eval(x))

        # iterate and add abstracts if they exist in semantic scholar data
        for index, row in self.orkg_df.iterrows():
            sem_field = row['semantic_field']

        if pd.isnull(row['abstract']):
            if bool(sem_field):
                self.orkg_df.at[index, 'abstract'] = sem_field['abstract']

        self.orkg_df['openalex_field'] = [api_data.get_openalex_data('https://doi.org/' + row['doi'], index)
                                          if not pd.isnull(row['doi']) else np.NaN
                                          for index, row in self.orkg_df.iterrows()]

        # make all non-existent abstract cells NaN
        self.orkg_df.loc[self.orkg_df['abstract'] == '{}', 'abstract'] = np.NaN

        # make all rows of openalex field a dict
        self.orkg_df['openalex_field'] = self.orkg_df['openalex_field'].apply(lambda x: ast.literal_eval(x))

        if pd.isnull(row['abstract']):
            if bool(sem_field):
                self.orkg_df.at[index, 'abstract'] = openalex_field['abstract']

        return self.orkg_df

    def _get_abstracts_from_orkg(self) -> pd.DataFrame:
        """
        Gets additional abstracts from the data provided by ORKG Abstracts:
        https://gitlab.com/TIBHannover/orkg/orkg-abstracts

        :return: dataframe with added abstracts from ORKG
        """

        orkg_df = pd.read_csv('data_processing/data/orkg_abstracts/orkg_papers.csv')
        orkg_df['title'] = [str(title).lower() for title in orkg_df['title']]
        self.orkg_df['orkg_abstract_doi'] = [get_orkg_abstract_doi(row['doi'], orkg_df)
                                             for index, row in self.orkg_df.iterrows()]
        self.orkg_df['orkg_abstract_title'] = [get_orkg_abstract_title(row['title'], orkg_df) for index, row in
                                               self.orkg_df.iterrows()]

        for index, row in self.orkg_df.iterrows():
            abst_doi = row['orkg_abstract_doi']
            abst_title = row['orkg_abstract_title']

            if pd.isnull(row['abstract']):
                if abst_doi != 'no_abstract_found' and is_english(abst_doi):
                    self.orkg_df.at[index, 'abstract'] = abst_doi
                elif abst_title != 'no_abstract_found' and is_english(abst_title):
                    self.orkg_df.at[index, 'abstract'] = abst_title

        self.orkg_df.drop(columns=['orkg_abstract_doi', 'orkg_abstract_title'])

        return self.orkg_df



