from typing import List

from fuzzywuzzy import fuzz



class DataValidation:
    """ Validate Data based on authors, doi and title """

    def __init__(self, level: int):
        self.validation_level = level

    def validate_data(
            self,
            api_title: str,
            orkg_title: str,
            api_authors: List[str],
            orkg_authors: str,
            api_doi: str,
            orkg_doi: str,
    ) -> bool:
        """
        Validates the api data based on title, author and doi.
        If one of the criterias are correct.
        The score is incremented by one.
        If score is >= validation_level the data from the api gets accepted.

        Parameters
        ----------
        api_title : str
        orkg_title : str
        api_authors : List[str]
        orkg_authors : str
        api_doi : str
        orkg_doi : str

        Returns
        -------
        bool
        """
        validation_score = 0

        validation_score += self._title_validation(api_title, orkg_title)
        validation_score += self._author_validation(api_authors, orkg_authors)
        if api_doi and orkg_doi:
            validation_score += self._doi_validation(api_doi, orkg_doi)

        api_title = api_title.encode("ascii", "ignore").decode()
        orkg_title = orkg_title.encode("ascii", "ignore").decode()


        return validation_score >= self.validation_level

    def _title_validation(self, api_title: str, orkg_title: str) -> int:
        score = 1 if fuzz.ratio(api_title.lower(), orkg_title.lower()) > 95 else 0
        return score

    def _doi_validation(self, api_doi: str, orkg_doi: str):
        score = 1 if fuzz.ratio(api_doi, orkg_doi) > 95 else 0
        return score

    def _author_validation(self, api_authors: List[str], orkg_authors: str) -> int:
        """
        Preprocesses string of api_authors and compares each author to validate the api data with fuzzy string matching

        Parameters
        ----------
        api_authors: List[str]
            scraped author data
        orkg_authors: str
            authors from orkg data

        Returns
        -------
        int
        """
        orkg_authors = str(orkg_authors).replace("'", "")
        orkg_authors = str(orkg_authors).replace(".", "")
        orkg_authors = str(orkg_authors).replace("[", "")
        orkg_authors = str(orkg_authors).replace("]", "")
        orkg_authors = str(orkg_authors).replace("et al", "")
        orkg_authors = str(orkg_authors).split(',')
        orkg_authors = [author.strip() for author in orkg_authors if not author.isdigit()]

        api_authors = [author for author in api_authors if not author.isdigit()]
        max_score = 0

        for real_author in orkg_authors:
            for api_author in api_authors:
                api_author = api_author.replace(".", "")
                api_author = api_author.replace("'", "")
                max_score = max(fuzz.ratio(api_author.lower(), real_author.lower()), max_score)

            if 60 < max_score < 85:
                for api_author in api_authors:
                    api_author = api_author.replace(".", "")
                    api_author = api_author.replace("'", "")
                    for name in api_author.split(' '):
                        if len(name) > 4:
                            max_score = max(fuzz.ratio(name.lower(), real_author.lower()), max_score)

        return int(max_score / 85)
