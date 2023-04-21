from typing import List, Dict
from orkg_data.Strategy import Strategy
from orkg import ORKG
from requests.exceptions import ConnectionError
import time
import requests
import json


class ORKGPyModule(Strategy):
    """
    Gets metadata of papers from the ORKG API.
    """

    def __init__(self):
        self.connector = ORKG(host="http://orkg.org/")
        self.predicate_url = 'http://www.orkg.org/orkg/api/statements/predicate/'
        self.subject_url = 'http://www.orkg.org/orkg/api/statements/subject/'

    def get_statement_by_predicate(self, predicate_id: str) -> Dict[str, List]:
        """
        Provides all paper ids and titles that have a research field.

        Parameters
        ----------
        predicate_id : str
            ID of "has research field"

        Returns
        -------
        Dict[str, list]
        """
        statement_data = {'paper': [], 'label': []}  # initializing the dictionary that will then be returned by the
        # function
        size = 20  # the default size of the batch of data fetched from ORKG (bigger sizes can cause connection
        # problems)
        response = requests.get(self.predicate_url + predicate_id + '?size=20' + '&page=' + str(0))  # the first
        # request, from which page_range will be obtained
        pages_range = json.loads(response.content)['totalPages']  # the page range that will be used in the for loop
        # to get all the statements

        for count in range(pages_range):
            try:
                response = requests.get(self.predicate_url + predicate_id + '?size=20' + '&page=' + str(count))

            except ConnectionError:
                time.sleep(60)
                response = requests.get(self.predicate_url + predicate_id + '?size=20' + '&page=' + str(count))

            if response.ok:
                content = json.loads(response.content)['content']

                for statement in content:
                    statement_data['paper'].append(statement['subject']['id'])
                    statement_data['label'].append(statement['object']['label'])

                if len(content) < int(size):
                    break

        print('Ready')
        return statement_data

    def get_statement_by_subject(self, paper_ids: List, meta_ids: Dict) -> Dict[str, list]:
        """
        Stores meta_infos for each paper in a Dict.
        Dict = {column_name: List[str], ...}

        Parameters
        ----------
        paper_ids : List[str]
            all paper_ids in orkg
        meta_ids : Dict
            relevant meta_ids (doi, ...)

        Returns
        -------
        Dict[str, list]
        """
        meta_infos = {key: [] for key in meta_ids.keys()}

        for key, meta_id in meta_ids.items():
            meta_ids[key] = meta_id.split('/')[-1]

        # structure: {predicate_id: predicate_string}
        look_up = {v: k for k, v in meta_ids.items()}

        for paper_id in paper_ids:

            try:
                response = self.connector.statements.get_by_subject(subject_id=paper_id, size=100, sort='id', desc=True)
            except ConnectionError:
                time.sleep(60)
                response = self.connector.statements.get_by_subject(subject_id=paper_id, size=100, sort='id', desc=True)

            if response.succeeded:
                content = response.content
                infos = {key: [] for key in meta_ids.keys()}

                for statement in content:

                    pred_id = statement['predicate']['id']
                    if pred_id in meta_ids.values():
                        infos[look_up[pred_id]].append(statement['object']['label'])

                    if not infos['title']:
                        infos['title'].append(statement['subject']['label'])

                # build lists in meta info dict for every predicate field
                for key, value in infos.items():
                    if len(value) == 0:
                        value = ""

                    if len(value) == 1:
                        value = value[0]

                    meta_infos[key].append(value)

        return meta_infos
