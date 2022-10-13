import pandas as pd
# from additional_api_data.doi_finder import DoiFinder
from util import process_abstract_string
from fuzzywuzzy import fuzz
from typing import List, Tuple, Dict
import requests
import urllib.parse
import json
import os
import time

from additional_api_data.api_scheduler import APIScheduler
from additional_api_data.data_validation import DataValidation

FILE_PATH = os.path.dirname(__file__)


class APIData:
    """
    Provides access to APIs of semantic scholar and crossref.\n
    DATA VALIDATION POLICY:
        - validate data from API with author check
        - scraped data won't be used if doi was scraped
        - query API with the scraped doi
        - just use scraped data if no doi and if fuzzy string matching is above 95

    See validation in function _author_validation()
    """

    def __init__(self, df: pd.DataFrame):
        """
        Gets initialized with obj reference of pandas dataframe that stores all papers.
        It is mandatory to provide a dataframe that contains the columns: title, author, url, publisher

        Parameters
        ----------
        df: pd.Dataframe
            obj reference
        """
        assert 'title' in df and 'author' in df and 'publisher' in df and 'url' in df
        self.df = df
        self.api_scheduler = APIScheduler()
        self.data_validation = DataValidation(level=2)

    def get_crossref_data(self, doi: str, index: int) -> Dict:
        """
        Provides dictionary of data collected from crossref API.

        Parameters
        ----------
        doi: str
            doi of queried paper
        index: int
            index of paper in pandas dataframe

        Returns
        -------
        Dict
            Dict that holds api data
        """
        if doi:
            crossref_url = 'https://api.crossref.org/works/' + str(doi)
        else:
            url_encoded_title = urllib.parse.quote_plus(self.df.at[index, 'title'])
            crossref_url = 'https://api.crossref.org/works?rows=5&query.bibliographic=' + url_encoded_title

        try:
            response = requests.get(crossref_url)

        except ConnectionError:
            time.sleep(60)
            response = requests.get(crossref_url)

        data_dict = {}

        if response.ok:
            content_dict_crossref = json.loads(response.content)
            message = content_dict_crossref['message']

            if not doi:
                message, paper_found = self._handle_crossref_title_api_data(index, message)
                if not paper_found:
                    return {}

            data_dict = self._process_api_data_crossref(message, index)

        return data_dict

    def get_semantic_scholar_data(self, doi: str, index: int) -> Dict:
        """
        Provides dictionary of data collected from semantic scholar api.

        Parameters
        ----------
        doi: str
            doi of queried paper
        index: int
            index of paper in pandas dataframe

        Returns
        -------
        Dict
            Dict that holds api data
        """
        if not doi:
            scraped_data = self._scrape_data_from_semantic(index)
            doi = scraped_data.get('doi', '')

            if not doi and 'author' in scraped_data and self.df.at[index, 'author']:
                return self._process_scraped_data(index, scraped_data)

        semantic_scholar_url = 'https://api.semanticscholar.org/v1/paper/' + str(doi)
        self.api_scheduler.update()

        try:
            response = requests.get(semantic_scholar_url)

        except ConnectionError:
            time.sleep(60)
            response = requests.get(semantic_scholar_url)

        data_dict = {}

        if response.ok:
            content_dict_scholar = json.loads(response.content)
            data_dict = self._process_api_data_semantic(content_dict_scholar, index)

        return data_dict

    def _handle_crossref_title_api_data(self, index: int, message: Dict) -> Tuple[Dict, bool]:
        """
        Provides a dict with api data and a boolean value if apper was found.
        Does a fuzzy string matching to see if apper was found in api call with paper title.
        If it finds a doi the api will be queried with the doi.

        Parameters
        ----------
        index: int
            index of paper in pandas dataframe
        message: Dict
            dict of all papers found that may match the title

        Returns
        -------
        Tuple[Dict, bool]
        """
        api_doi = ''
        paper_found = True

        for item in message.get('items', []):
            if self.df.at[index, 'title'].lower() == item.get('title', '')[0].lower():
                api_doi = item.get('DOI', '')
                message = item
                break
            elif fuzz.ratio(self.df.at[index, 'title'].lower(), item.get('title', '')[0].lower()) > 95:
                api_doi = item.get('DOI', '')
                message = item
                break

        if api_doi:
            response = requests.get('https://api.crossref.org/works/' + api_doi)
            if response.ok:
                content_dict_crossref = json.loads(response.content)
                message = content_dict_crossref['message']

        # if items is key of message dict no paper was found
        if 'items' in message.keys() or not message:
            paper_found = False

        return message, paper_found

    def _process_api_data_crossref(self, message, index) -> Dict:
        """
        Provides data dict from processed data of crossref api.
        Adds publisher, doi and url if available.

        Parameters
        ----------
        message: Dict
            dict with paper data
        index: int
            index of paper in pandas dataframe

        Returns
        -------
        Tuple[str, List]
        """
        # authors = [person.get('given', '') + ' ' + person.get('family', '') for person in message.get('author', [])]

        #valid_data = self.data_validation.validate_data(
         #   message.get('title', '')[0], self.df.at[index, 'title'], authors, self.df.at[index, 'author'],
          #  message.get('DOI', ''), self.df.at[index, 'doi']
       # )

        data_dict = {}
        # if valid_data:
        abstract = process_abstract_string(message.get('abstract', ''))

        data_dict['abstract'] = abstract
        data_dict['crossref_field'] = message.get('subject', []),
        data_dict['publisher'] = message.get('container-title', '')
        data_dict['doi'] = message.get('DOI', '')
        data_dict['url'] = message.get('URL', '')

        return data_dict

    def _scrape_data_from_semantic(self, index: int) -> Dict:
        """
        Provides scraped data from semantic scholar.
        Uses the class DoiFinder to scrape data with selenium

        Parameters
        ----------
        index:
            entry in dataframe of the paper

        Returns
        -------
        Dict
        """
        scraped_data = {}
        doi_finder = DoiFinder()

        try:
            scraped_data = doi_finder.scrape_data_from_semantic_scholar(self.df.at[index, 'title'])
        except Exception as e:
            print(f"Exception in DOIFinder{e}")

        doi_finder.close_session()
        return scraped_data

    def _process_scraped_data(self, index: int, scraped_data: Dict) -> Dict:
        """
        Handles the scraped data.
        Be careful this data is just validated by the author names

        Parameters
        ----------
        index: int
            index of dataframe entry
        scraped_data: Dict
            Dict that contains scraped data

        Returns
        -------
        Dict
        """
        valid_data = self.data_validation.validate_data(
            scraped_data.get('title', ''), self.df.at[index, 'title'], scraped_data.get('author', ''),
            self.df.at[index, 'author'], scraped_data.get('DOI', ''), self.df.at[index, 'doi']
        )

        data_dict = {}
        if valid_data and scraped_data:
            data_dict['semantic_field'] = scraped_data.get('research field', '')
            data_dict['publisher'] = scraped_data.get('publisher', '')
            data_dict['abstract'] = process_abstract_string(scraped_data.get('abstract', ''))

        return data_dict

    def _process_api_data_semantic(self, content_dict_scholar: Dict, index: int) -> Dict:
        """
        Provides abstract and research field data from processed data of semantic scholar api.
        Adds edit publisher, doi and url if available.

        Parameters
        ----------
        content_dict_scholar: Dict
            Dict of retireved data from semantic scholar api
        index: int
            entry in dataframe of the paper

        Returns
        -------
        Tuple[str, List]
        """
        author_names = [person.get('name', '') for person in content_dict_scholar.get('authors', [])]

       # valid_data = self.data_validation.validate_data(
        #    content_dict_scholar.get('title', ''), self.df.at[index, 'title'], author_names,
         #   self.df.at[index, 'author'], content_dict_scholar.get('doi', ''), self.df.at[index, 'doi']
        #)

        data_dict = {}
        #if valid_data:
        abstract = process_abstract_string(content_dict_scholar.get('abstract', ''))
        data_dict['abstract'] = abstract
        data_dict['semantic_field'] = content_dict_scholar.get('fieldsOfStudy', [])
        data_dict['publisher'] = content_dict_scholar.get('venue', '')
        data_dict['doi'] = content_dict_scholar.get('doi', '')
        data_dict['url'] = content_dict_scholar.get('url', '')

        return data_dict
