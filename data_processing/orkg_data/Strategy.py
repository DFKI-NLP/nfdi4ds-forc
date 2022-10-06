from abc import ABC, abstractmethod
from typing import Dict, List


class Strategy(ABC):
    """
    Interface that describes the methods needed to get metadata for all papers in the orkg
    """

    @abstractmethod
    def get_statement_by_predicate(self, predicate_id: str) -> Dict[str, list]:
        """
        Method that provides all paper ids and titles that have a research field.

        Parameters
        ----------
        predicate_id : str
            ID of "has research field"

        Returns
        -------
        Dict[str, list]
        """
        pass

    @abstractmethod
    def get_statement_by_subject(self, paper_ids: List[str], meta_ids: Dict) -> Dict[str, list]:
        """
        Stores meta_infos for each paper in a Dict.
        Dict = {paper:

        Parameters
        ----------
        paper_ids : List[str]
        meta_ids : Dict

        Returns
        -------
        Dict[str, list]
        """
        pass
