import ast
import csv
import json
import numpy as np
import pandas as pd
from typing import Dict
from tqdm import tqdm
from additional_api_data.api_data import APIData
from orkg_data.Strategy import Strategy
from orkg_data.orkgPyModule import ORKGPyModule
from data_cleaning_utils import process_abstract_string, remove_non_english, is_english, standardize_doi, \
    cleanhtml_titles, remove_extra_space, drop_non_papers, remove_duplicates, get_orkg_abstract_doi, \
    get_orkg_abstract_title, parse_author

from parsel import Selector
from playwright.sync_api import sync_playwright

from calendar import month_abbr, calendar
from fuzzywuzzy import fuzz


class ORKGData:
    """
    Provides functionality to:
        - Load metadata for papers from ORKG.
        - Clean orkg data.
        - Query missing abstracts using: Crossref, S2AG, ORKG Abstract Finder repo, or ResearchGate.
        - For papers labelled as 'Science' in ORKG, get the correct label from Crossref/Semantic Scholar.
        - Integrate manual re-labeling of remaining papers tagged as 'Science'.
        - Map research fields from Crossref and SemanticScholar to ORKG research fields.
        - Merge research fields to reduce their number.
    """

    def __init__(self, strategy: Strategy) -> None:
        """
        Load data from ORKG API or rdfDump

        Parameters
        ----------
        strategy :
            Strategy to load Data
        """
        self._strategy = ORKGPyModule()
        self.scheduler = []
        self.paper_index_list = []

        self.predicate_id = 'P30' # the id of the predicate 'research field' in ORKG
        self.meta_ids = {
            'doi': 'http://orkg.org/orkg/predicate/P26',
            'author': 'http://orkg.org/orkg/predicate/P27',
            'publication month': 'http://orkg.org/orkg/predicate/P28',
            'publication year': 'http://orkg.org/orkg/predicate/P29',
            'title': 'http://www.w3.org/2000/01/rdf-schema#label',
            'publisher': 'http://orkg.org/orkg/predicate/HAS_VENUE',
            'url': 'http://orkg.org/orkg/predicate/url'
        }

        self.df = pd.DataFrame(columns=['abstract', 'author', 'doi', 'url', 'publication month', 'publication year',
                                        'title', 'paper_id', 'publisher', 'crossref_field', 'semantic_field', 'label'])

        self.data_stats = {'num_papers': 0, 'num_dois': [], 'num_publisher': [], 'num_science_labels': [],
                           'num_urls': []}

    def load_label_data(self) -> pd.DataFrame:
        """
        Initializes dataframe with orkg data.

        Returns dataframe with raw ORKG data
        -------
        """
        predicate_statements = self._strategy.get_statement_by_predicate(self.predicate_id)
        self.df['label'] = predicate_statements['label']
        self.df['paper_id'] = predicate_statements['paper']

        subject_statements = self._strategy.get_statement_by_subject(predicate_statements['paper'], self.meta_ids)
        for column_name, values in subject_statements.items():
            if column_name == 'abstract' or column_name == 'title':
                values = [process_abstract_string(value) for value in values]
            self.df[column_name] = values

        if 'paper_id' in self.df:
            del self.df['paper_id']

        return self.df

    def clean_orkg_data(self) -> pd.DataFrame:
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
        self.df = drop_non_papers(self.df)
        self.df['title'] = self.df['title'].apply(lambda x: remove_extra_space(x))
        self.df['title'] = self.df['title'].apply(lambda x: cleanhtml_titles(x))
        self.df['doi'] = self.df['doi'].apply(lambda x: standardize_doi(x))
        self.df = remove_duplicates(self.df)
        self.df = remove_non_english(self.df)
        self.df = self.parse_authors_orkg(self.df)

        return self.df

    def get_abstracts_from_apis(self) -> pd.DataFrame:
        """
        Get abstracts from crossref and semantic scholar using the APIData class
        :return: dataframe with added abstracts
        """
        self.df = APIData(self.df)
        self.df['crossref_field'] = [self.df.get_crossref_data(row['doi'], index)
                                     for index, row in self.df.iterrows()]
        self.df['abstract'] = [ab['abstract'] if ab != {} else {} for ab in self.df['crossref_field']]

        self.df['semantic_field'] = [self.df.get_semantic_scholar_data(row['doi'], index)
                                     for index, row in self.df.iterrows()]

        # make all non-existent abstract cells NaN
        self.df.loc[self.df['abstract'] == '{}', 'abstract'] = np.NaN

        # make all rows of semantic field a dict
        self.df['semantic_field'] = self.df['semantic_field'].apply(lambda x: ast.literal_eval(x))

        # iterate and add abstracts if they exist in semantic scholar data
        for index, row in self.df.iterrows():
            sem_field = row['semantic_field']

        if pd.isnull(row['abstract']):
            if bool(sem_field):
                self.df.at[index, 'abstract'] = sem_field['abstract']

        return self.df

    def get_abstracts_from_orkg(self) -> pd.DataFrame:
        """
        Gets additional abstracts from the data provided by ORKG Abstracts:
        https://gitlab.com/TIBHannover/orkg/orkg-abstracts

        :return: dataframe with added abstracts from ORKG
        """

        orkg_df = pd.read_csv('data_processing/data/orkg_abstracts/orkg_papers.csv')
        orkg_df['title'] = [str(title).lower() for title in orkg_df['title']]
        self.df['orkg_abstract_doi'] = [get_orkg_abstract_doi(row['doi'], orkg_df)
                                        for index, row in self.df.iterrows()]
        self.df['orkg_abstract_title'] = [get_orkg_abstract_title(row['title'], orkg_df) for index, row in
                                          self.df.iterrows()]

        for index, row in self.df.iterrows():
            abst_doi = row['orkg_abstract_doi']
            abst_title = row['orkg_abstract_title']

            if pd.isnull(row['abstract']):
                if abst_doi != 'no_abstract_found' and is_english(abst_doi):
                    self.df.at[index, 'abstract'] = abst_doi
                elif abst_title != 'no_abstract_found' and is_english(abst_title):
                    self.df.at[index, 'abstract'] = abst_title

        self.df.drop(columns=['orkg_abstract_doi', 'orkg_abstract_title'])

        return self.df

    def scrape_researchgate_publications(self, query: str) -> Dict:
        """
        Scrapes ResearchGate publications for a given query. The query is the title of the paper,
        for which the abstract and/or other metadate is to be found.
        :param query: query to search for (the title of a publication)
        :return: dictionary with scraped data in the following format:
        { 'title': 'title of the paper',
          'link': 'link to the paper on ResearchGate',
          'publication_type': 'type of publication (e.g. Article)',
          'publication_date': 'date of publication (e.g. 'Aug 2019'),
          'publication_doi': 'doi of the publication',
          'authors': ['list of authors'],
          'abstract': 'abstract of the publication'
        }
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, slow_mo=50)
            page = browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/108.0.0.0 Safari/537.36")

            while True:
                page.goto(f"https://www.researchgate.net/search/publication?q={query}")
                selector = Selector(text=page.content())
                publication = selector.css(".nova-legacy-c-card__body--spacing-inherit")
                title = publication.css(
                    ".nova-legacy-v-publication-item__title .nova-legacy-e-link--theme-bare::text").get()
                title_link = f'https://www.researchgate.net/{publication.css(".nova-legacy-v-publication-item__title .nova-legacy-e-link--theme-bare::attr(href)").get()}'
                publication_type = publication.css(".nova-legacy-v-publication-item__badge::text").get()
                publication_date = publication.css(
                    ".nova-legacy-v-publication-item__meta-data-item:nth-child(1) span::text").get()
                publication_doi = publication.css(
                    ".nova-legacy-v-publication-item__meta-data-item:nth-child(2) span").xpath(
                    "normalize-space()").get()
                authors = publication.css(".nova-legacy-v-person-inline-item__fullname::text").getall()

                publication_page = browser.new_page(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, "
                               "like Gecko) Chrome/108.0.0.0 Safari/537.36")

                publication_page.goto(title_link)
                selector = Selector(text=publication_page.content())
                publication = selector.css(".nova-legacy-c-card__body--spacing-inherit")
                abstract = publication.css(".research-detail-middle-section__abstract::text").get()

                result = {
                    "title": title,
                    "link": title_link,
                    "publication_type": publication_type,
                    "publication_date": publication_date,
                    "publication_doi": publication_doi,
                    "authors": authors,
                    "abstract": abstract
                }

                if result['title'] is not None:
                    result['fuzz_ratio'] = fuzz.ratio(result['title'].lower(), query)

                print(f"Added the following results: {result}")

                browser.close()
                return result

    def get_metadata_from_researchgate(self, orkg_df):
        """
        Gets additional metadata from ResearchGate for the papers in the dataframe. The function then adds the following
        missing metadata:
        - abstracts
        - publication date (for missing dates)
        - publication date (for papers incorrectly tagged with Jan 2000)
        - DOI
        :return: dataframe with missing metadata added from ResearchGate
        """
        #orkg_df['researchgate_metadata'] = [self.scrape_researchgate_publications(row['title'])
       #                                     for index, row in orkg_df.iterrows()]
        #orkg_df.to_csv('data_processing/data/orkg_abstracts/orkg_papers_after_webscraping.csv', index=False)

        for index, row in orkg_df.iterrows():
            if row['researchgate_metadata']['fuzz_ratio'] >= 90:
                metadata = row['researchgate_metadata']
                df.at[index, 'abstract'] = self.get_researchgate_abstract(row, metadata)
                df.at[index, 'publication month'], df.at[index, 'publication year'] = \
                    self.get_researchgate_date(row, metadata)
                df.at[index, 'doi'] = self.get_researchgate_doi(row, metadata)

                if (row['publication year'] == 2000.0) & (row['publication month'] == '1'):
                    df.at[index, 'publication month'], df.at[index, 'publication year'] = \
                        self.correct_2000_date(row, metadata)

        return orkg_df

    def get_researchgate_abstract(self, row, metadata):
        """
        Gets missing abstracts from ResearchGate
        :param row: one row of the dataframe with papers including the column 'researchgate_metadata'.
        :return: the abstract of that row from ResearchGate, only if the abstract is missing.
        """
        if pd.isnull(row['abstract']):
            if metadata['abstract'] is not None:
                return metadata['abstract']
        return row['abstract']

    def get_researchgate_date(self, row, metadata):
        """
        Gets missing publication dates from ResearchGate
        :param row: one row of the dataframe with papers including the column 'researchgate_metadata'.
        :param metadata: the metadata of that row from ResearchGate
        :return: the publication month and year of that row from ResearchGate, only if they are missing from
        the ORKG data.
        """
        orkg_month = row['publication month']
        orkg_year = row['publication year']
        if metadata['publication_date'] is not None:
            date = metadata['publication_date']
            month, year = date.split(' ')
            month = list(month_abbr).index(month)
            year = float(year)

        if pd.isnull(orkg_month):
            orkg_month = month

        if pd.isnull(orkg_year):
            orkg_year = year

        return orkg_month, orkg_year

    def correct_2000_date(self, row, metadata):
        """
        Corrects publications with the publication year 2000 based on the data scraped from ResearchGate.
        :param row: one row of the dataframe with papers including the column 'researchgate_metadata', that is
        suspicious of having an incorrect date
        :param metadata: the metadata of that row from ResearchGate
        :return: the publication month and year of that row from ResearchGate
        """
        if metadata['publication_date'] is not None:
            date = metadata['publication_date']
            month, year = date.split(' ')
            month = list(month_abbr).index(month)
            year = float(year)
        else:
            month = None
            year = None

        return month, year

    def get_researchgate_doi(self, row, metadata):
        """
        Returns missing doi as found in ResearchGate.
        param row: one row of the dataframe with papers including the column 'researchgate_metadata'
        :param metadata: the metadata of that row from ResearchGate
        :return: the missing doi if found.
        """
        if pd.isnull(row['doi']):
            if metadata['publication_doi'] is not None:
                if metadata['publication_doi'].startswith('DOI:'):
                    return metadata['publication_doi'][5:]
                elif not metadata['publication_doi'].startswith('ISBN:'):
                    return metadata['publication_doi']
        return row['doi']

    def convert_science_labels(self) -> pd.DataFrame:
        """
        converts 'Science' labels in orkg data to the appropriate label from crossref and semantic scholar
        according to mapping files

        :return: dataframe converted Science labels
        """
        # load df only with science labels
        science_df = self.df.query('label == "Science"')
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
                        self.df.at[index, 'label'] = cross_ref_mappings[label]

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
                            self.df.at[index, 'label'] = semanticschol_mappings[label]

        return self.df

    def export_science_labels(self, orkg_df, export_path="data_processing/data/science_labels.csv") -> None:
        """
        Exports science labels to .csv in preparation for manual re-labelling.
        :parameter orkg_df: dataframe with science labels
        :parameter export_path: path to export the science labels to
        """
        science_df = self.df.query('label == "Science"')
        science_df.to_csv(export_path, index=False)

    def relabel_science_manual(self, orkg_df,
                               xlsx_path="/Users/rayaabuahmad/Documents/dfki-work/merged_data_relabeling_science.xlsx")\
            -> pd.DataFrame:
        """
        relabels the remaining 'Science' labels manually by:
        1. Importing the relabeled file in .xlsx format; in which nee labels are added in the 'new_label' column.
        2. Merging the relabeled dataframe with the original dataframe.
        :return:

        """
        science_relabeled_df = pd.read_excel(xlsx_path)
        orkg_df['label'] = [self.get_new_label(row, science_relabeled_df) for index, row in
                                     orkg_df.iterrows()]

        return orkg_df

    def get_new_label(self, row, science_relabeled_df):
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


    def reduce_rf(self) -> pd.DataFrame:
        """
        Re-label Arts and Humanities sub-fields to the higher level class +
        Reduces labels from about 300 to about 50.

        :return: dataframe reduced to 51 labels
        """
        # re-label arts&humanities fields
        with open('data_processing/data/mappings/arts_humanities_field.csv', newline='') as f:
            reader = csv.reader(f)
            arts_humanities = list(reader)

        arts_humanities = [item for sublist in arts_humanities for item in sublist]

        self.df['label'] = ['Arts and Humanities' for item in self.df['label'] if item.isin(arts_humanities)]

        # reduce remaining research fields
        path = 'data_processing/data/mappings/rf_reduction.json'
        with open(path, 'r') as infile:
            mappings_reduction = json.load(infile)

        for index, row in self.df.iterrows():
            if row['label'] in mappings_reduction.keys():
                self.df.at[index, 'label'] = mappings_reduction[row['label']]

        return self.df

    def parse_authors_orkg(self, orkg_df):
        """
        takes the orkg_df and adds a column, authors_parsed, with the same authors parsed in a list
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

    @property
    def strategy(self) -> Strategy:
        """Load Strategy for ORKG Data"""
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: Strategy) -> None:
        self._strategy = strategy


def orkg_data_pipeline():
    """
    A function that applies the ORKG data pipeline and saves the result in
    "data_processing/data/orkg_processed_data.csv"
    :return:
    """
    orkg_data = ORKGData(ORKGPyModule())
    orkg_df = orkg_data.load_label_data()
    orkg_df = orkg_data.clean_orkg_data()
    orkg_df = orkg_data.get_abstracts_from_apis()
    orkg_df = orkg_data.get_abstracts_from_orkg()
    orkg_df = orkg_data.convert_science_labels()
    orkg_df = orkg_data.reduce_rf()
    orkg_df.to_csv('data_processing/data/orkg_processed_data.csv', index=False)


def remove_doi_dups(data_df):
    """
    removes rows of df where the doi is duplicated (keeps first one) and saves the data into a csv
    :param data_df:
    :return: -
    """
    data_df = data_df[(~data_df['doi'].duplicated()) | data_df['doi'].isna()]
    data_df.to_csv('data_processing/data/orkg_data_science_conversion_no_dups.csv', index=False)


def get_abstracts_from_orkg(df):
    """
    Gets additional abstracts from the data provided by ORKG Abstracts:
    https://gitlab.com/TIBHannover/orkg/orkg-abstracts
    """

    orkg_df = pd.read_csv('data_processing/data/orkg_abstracts/orkg_papers.csv')
    orkg_df['title'] = [str(title).lower() for title in orkg_df['title']]
    df['orkg_abstract_doi'] = [get_orkg_abstract_doi(row['doi'], orkg_df)
                               for index, row in df.iterrows()]
    df['orkg_abstract_title'] = [get_orkg_abstract_title(row['title'], orkg_df) for index, row in
                                 df.iterrows()]

    for index, row in df.iterrows():
        abst_doi = row['orkg_abstract_doi']
        abst_title = row['orkg_abstract_title']

        if pd.isnull(row['abstract']):
            if abst_doi != 'no_abstract_found' and is_english(abst_doi):
                df.at[index, 'abstract'] = abst_doi
            elif abst_title != 'no_abstract_found' and is_english(abst_title):
                df.at[index, 'abstract'] = abst_title

    df = df.drop(columns=['orkg_abstract_doi', 'orkg_abstract_title'])

    return df


if __name__ == '__main__':
    df = pd.read_csv('data_processing/data/orkg_data_webscraped_fuzz.csv')
    df['researchgate_metadata'] = [ast.literal_eval(metadata) for metadata in df['researchgate_metadata']]
    orkg_data = ORKGData(ORKGPyModule())
    orkg_df = orkg_data.get_metadata_from_researchgate(df)
    orkg_df.to_csv('data_processing/data/orkg_data_webscraped_NEW.csv', index=False)
