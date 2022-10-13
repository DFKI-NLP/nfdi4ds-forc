from selenium.webdriver import Chrome, DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
import time
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
import random
from typing import Dict
from fuzzywuzzy import fuzz
import os


EUROPE = ['Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
          'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
          'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
          'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden']

FIELDS = ['Medicine', 'Biology', 'Chemistry', 'Engineering', 'Computer Science', 'Physics', 'Materials Science',
          'Mathematics', 'Psychology', 'Economics', 'Political Science', 'Business', 'Geology', 'Sociology',
          'Geography', 'Environmental Science', 'Art', 'History', 'Philosophy']

logger = MyLogger('doi_finder').logger
FILE_PATH = os.path.dirname(__file__)


class DoiFinder:
    """
    class that manages to find dois and meta data of papers on semantic schoolar
    """

    def __init__(self):
        """
        make sure Chrome Driver is available in data folder otherwise download before use from
        https://sites.google.com/a/chromium.org/chromedriver/downloads
        """
        self.proxy_list = []
        # self._set_proxies()
        self.proxy_limit = 10
        self.count = 0

        self.opts = Options()
        self.opts.add_argument('--headless')
        assert self.opts.headless  # Operating in headless mode
        self.browser = Chrome(executable_path=os.path.join(FILE_PATH, "../data/chromedriver.exe"), options=self.opts)

    def _set_proxies(self):
        """
        set proxies to cover IP and run requests from different IPs to avoid getting blocked

        Returns
        -------
        """
        req_proxy = RequestProxy()
        proxies = req_proxy.get_proxy_list()
        for proxy in proxies:
            if proxy.country in EUROPE:
                self.proxy_list.append(proxy)

        random_proxy = random.choice(self.proxy_list).get_address()
        DesiredCapabilities.CHROME['proxy'] = {
            "httpProxy": random_proxy,
            "proxyType": "MANUAL",
        }

    def _restart_proxy(self):
        """
        restarts proxies after fix limit of requests was reached and randomly select new ones

        Returns
        -------
        """
        self.browser.quit()

        random_proxy = random.choice(self.proxy_list).get_address()
        DesiredCapabilities.CHROME['proxy'] = {
            "httpProxy": random_proxy,
            "proxyType": "MANUAL",
        }
        self.browser = Chrome(executable_path="../data/chromedriver.exe", options=self.opts)

    def scrape_data_from_semantic_scholar(self, title: str) -> Dict:
        """
        uses selenium and the paper title to find paper meta infos on semantic schoolar
        scrapes this meta data into a

        Parameters
        ----------
        title: str
            paper title

        Returns
        -------
        Dict
        """
        data = {}

        self.browser.get('https://www.semanticscholar.org/')

        # search paper title
        input_form = self.browser.find_element_by_class_name('form-input')
        input_form.clear()
        input_form.send_keys(title)
        input_form.submit()
        time.sleep(5)

        links = self.browser.find_elements_by_xpath("//a[@data-selenium-selector='title-link']")
        time.sleep(1)

        for link in links:
            time.sleep(1)

            # scrape data if fuzzy string matching of paper title and title found  is accurate enough
            if fuzz.ratio(link.text.lower(), title.lower()) > 95:

                data['title'] = link.text
                url = link.get_attribute('href')
                time.sleep(1)
                self.browser.get(url)
                time.sleep(5)

                try:
                    data['doi'] = self.browser.find_element_by_class_name('doi__link').text
                except NoSuchElementException:
                    logger.info('No DOI for: ' + title)

                try:
                    try:
                        self.browser.find_element_by_xpath(
                            "//a[@data-selenium-selector='text-truncator-toggle']").click()
                    except NoSuchElementException:
                        pass
                    except ElementClickInterceptedException:
                        pass

                    data['abstract'] = self.browser.find_element_by_class_name('abstract__text').text

                    try:
                        data['publisher'] = self.browser.find_element_by_xpath(
                            "//span[@data-heap-id='paper-meta-journal']").text
                    except NoSuchElementException:
                        logger.info('No PUBLISHER for:' + title)

                    research_fields = self.browser.find_elements_by_class_name("paper-meta-item")
                    for field in research_fields:
                        if field.text in FIELDS:
                            data['research field'] = field.text

                    data['year'] = self.browser.find_element_by_xpath(
                        "//span[@data-selenium-selector='paper-year']").text

                    data['author'] = []
                    self.browser.find_element_by_class_name("more-authors-label").click()
                    authors = self.browser.find_elements_by_xpath("//span[@data-selenium-selector='author-list']")
                    authors = authors[:4] if len(authors) > 4 else authors
                    for author in authors:
                        data['author'].append(author.text.replace(',', '').strip())

                except NoSuchElementException:
                    logger.info('No Data for: ' + title)
                    break

                break

        return data

    def close_session(self):
        """ close headless selenium browser"""
        self.browser.quit()


if __name__ == '__main__':
    finder = DoiFinder()
    web_data = finder.scrape_data_from_semantic_scholar(
        'Benefits of Bt cotton use by smallholders farmers in South Africa')
    print(web_data)
    finder.close_session()
