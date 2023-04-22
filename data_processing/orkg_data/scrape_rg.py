import pandas as pd
from parsel import Selector
from playwright.sync_api import sync_playwright

from calendar import month_abbr, calendar
from fuzzywuzzy import fuzz


class RGScraper:
    """
    Class to scrape ResearchGate for missing metadata (e.g. abstracts, publication dates, DOIs).
    """

    def __init__(self, orkg_df):
        self.orkg_df = orkg_df

    def run(self) -> pd.DataFrame:
        """
        Runs the scraper.
        :return: dataframe with missing metadata added from ResearchGate
        """
        return self._get_metadata_from_researchgate()

    def _scrape_researchgate_publications(self, query: str) -> Dict:
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

    def _get_metadata_from_researchgate(self) -> pd.DataFrame:
        """
        Gets additional metadata from ResearchGate for the papers in the dataframe. The function then adds the following
        missing metadata:
        - abstracts
        - publication date (for missing dates)
        - publication date (for papers incorrectly tagged with Jan 2000)
        - DOI
        :return: dataframe with missing metadata added from ResearchGate
        """
        self.orkg_df['researchgate_metadata'] = [self._scrape_researchgate_publications(row['title'])
                                                 for index, row in self.orkg_df.iterrows()]
        self.orkg_df.to_csv('data_processing/data/orkg_abstracts/orkg_papers_after_webscraping.csv', index=False)

        # Add missing metadata from ResearchGate: missing abstracts, missing publication dates, DOI
        for index, row in self.orkg_df.iterrows():
            if row['researchgate_metadata']['fuzz_ratio'] >= 90:
                metadata = row['researchgate_metadata']
                self.orkg_df.at[index, 'abstract'] = self._get_researchgate_abstract(row, metadata)
                self.orkg_df.at[index, 'publication month'], df.at[index, 'publication year'] = \
                    self._get_researchgate_date(row, metadata)
                self.orkg_df.at[index, 'doi'] = self._get_researchgate_doi(row, metadata)

                if (row['publication year'] == 2000.0) & (row['publication month'] == '1'):
                    self.orkg_df.at[index, 'publication month'], self.orkg_df.at[index, 'publication year'] = \
                        self._correct_2000_date(row, metadata)

        return self.orkg_df

    def _get_researchgate_abstract(self, row: pd.core.series.Series, metadata: Dict) -> pd.core.series.Series:
        """
        Gets missing abstracts from ResearchGate
        :param row: one row of the dataframe with papers including the column 'researchgate_metadata'.
        :return: the abstract of that row from ResearchGate, only if the abstract is missing.
        """
        if pd.isnull(row['abstract']):
            if metadata['abstract'] is not None:
                return metadata['abstract']

        return row['abstract']

    def _get_researchgate_date(self, row: pd.core.series.Series, metadata: Dict) -> (str, str):
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

    def s(self, metadata: Dict) -> (str, str):
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

    def m(self, row: pd.core.series.Series, metadata: Dict) -> str:
        """
        Returns missing doi as found in ResearchGate.
        :param row: one row of the dataframe with papers including the column 'researchgate_metadata'
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
