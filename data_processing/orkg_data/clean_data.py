import pandas as pd
from typing import Dict

from data_cleaning_utils import remove_non_english, standardize_doi, cleanhtml_titles, remove_extra_space, \
    drop_non_papers, remove_duplicates, parse_author


class ORKGDataCleaner:

    def __init__(self, orkg_df: pd.DataFrame):
        self.orkg_df = orkg_df

    def run(self) -> pd.DataFrame:
        """
        Cleans orkg raw data in the following steps:
        1. Removes non-papers (papers with no adequate title and no other information).
        2. Removes extra space from titles.
        3. Cleans html and other code remnants from titles.
        4. Standardizes doi (no "https://doi.org" prefix).
        5. Removes duplicate papers (according to title) and keeps the one with less NaN cells.
        6. Removes non-English papers.
        7. Parses authors into a standardized format.

        :return: cleaned dataframe
        """
        self.orkg_df = drop_non_papers(self.orkg_df)
        self.orkg_df['title'] = self.orkg_df['title'].apply(lambda x: remove_extra_space(x))
        self.orkg_df['title'] = self.orkg_df['title'].apply(lambda x: cleanhtml_titles(x))
        self.orkg_df['doi'] = self.orkg_df['doi'].apply(lambda x: standardize_doi(x))
        self.orkg_df = remove_duplicates(self.orkg_df)
        self.orkg_df = remove_non_english(self.orkg_df)
        self.orkg_df = self._parse_authors_orkg(self.orkg_df)

        return self.df

    def _parse_authors_orkg(self, orkg_df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes the orkg_df and adds the column 'authors_parsed' with the same authors parsed in a list.
        """
        orkg_df['authors_parsed'] = ''
        for index, row in orkg_df.iterrows():

            if not pd.isna(row['author']):
                if row['author'].startswith('['):
                    author_list = ast.literal_eval(row['author'])
                    authors_list_parsed = []
                    for author in author_list:
                        authors_list_parsed.append(parse_author(author))
                    orkg_df.at[index, 'authors_parsed'] = authors_list_parsed
                else:
                    orkg_df.at[index, 'authors_parsed'] = parse_author(row['author'])
        return orkg_df
