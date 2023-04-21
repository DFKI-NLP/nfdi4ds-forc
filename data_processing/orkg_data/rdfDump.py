from data_processing.orkg_data.Strategy import Strategy
import rdflib
from typing import List, Dict


class RDFDump(Strategy):
    """
    Gets metadata for papers from the RDF Dump of ORKG.
    """
    def __init__(self, parse=True):
        """
        Parameters
        ----------
        parse: bool
            bool that decides wheather graph gets parsed from rdf dump
        """
        self.graph = rdflib.Graph()
        if parse:
            self.graph.parse("data/dump.nt", format="turtle")

    def get_statement_by_predicate(self, predicate_id: str) -> Dict[str, list]:
        """
        Provides all paper ids and titles that have a research field from rdf dump.

        Parameters
        ----------
        predicate_id : str
            ID of "has research field"

        Returns
        -------
        Dict[str, list]
        """
        predicate = rdflib.URIRef(predicate_id)
        statement_data = {'paper': [], 'label': []}

        for sub, pred, obj in self.graph.triples((None, predicate, None)):
            statement_data['paper'].append(sub)
            # map list of URIREFs to list of strings
            statement_data['label'].append(self.id_to_string([obj])[0])

        return statement_data

    def get_statement_by_subject(self, paper_ids: List[str], meta_ids: Dict) -> Dict[str, list]:
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
        look_up = {v: k for k, v in meta_ids.items()}

        for paper_id in paper_ids:
            subject = rdflib.URIRef(paper_id)
            infos = {key: [] for key in meta_ids.keys()}

            # read statements as triples from graph
            for sub, pred, obj in self.graph.triples((subject, None, None)):
                pred = str(pred)

                if pred in meta_ids.values():
                    meta_key = look_up[pred]

                    # map URIREF to string
                    if type(obj) is rdflib.term.URIRef and obj:
                        infos[meta_key].append(self.id_to_string([obj])[0])

                    if type(obj) is rdflib.term.Literal and obj:
                        infos[meta_key].append(str(obj))

            # append for each key the Literal values
            for key, value in infos.items():
                if len(value) == 0:
                    value = ""
                if len(value) == 1:
                    value = value[0]
                meta_infos[key].append(value)

        return meta_infos

    def id_to_string(self, ids: List) -> List[str]:
        """
        maps all URIREFs to list of strings

        Parameters
        ----------
        ids: List[str]
            contains the URIREF(s)

        Returns
        -------
        List[str]
        """
        string_list = []

        for subject in ids:
            for sub, pred, obj in self.graph.triples((subject, None, None)):
                if type(obj) is rdflib.term.Literal:
                    string_list.append(str(obj))

        return string_list
