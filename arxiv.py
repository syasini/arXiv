"""
A parallel scraper designed for scraping arXiv and inspirehep records
"""

__author__ = "Siavash Yasini"
__email__ = "siavash.yasini@gmail.com"

import requests
import re
from time import sleep
from bs4 import BeautifulSoup
from itertools import chain
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse as parsedate


class Paper:
    """Paper object contains the information of an individual record


    attributes
    ----------
    from_: string [YYYY-mm-dd]
        The start date for the harvest
    to_: string [YYYY-mm-dd]
        The end date for the harvest
    set_: string
        The main category to harvest
    fields: string or list of strings
        can be a list any of the below items or set to "everything"

        id
        title
        abstract
        set
        categories
        authors

        created
        updated

        comments
        doi
        datestamp
        setSpec

    methods
    -------

    """
    def __init__(self,
                 from_=None,
                 to_=None,
                 set_="physics:astro-ph",
                 fields="everything"):

        self.set_dict = ["physics:astro-ph"]

        # check the from_ date
        if from_ is None:
            self.from_ = self.days_back(1)
        else:
            self.from_ = self.check_date_format(from_)

        # check the to_ date
        if to_ is None:
            self.to_ = self.days_back(0)
        else:
            self.to_ = self.check_date_format(to_)

        # check the main category setSpec
        if set_ not in self.set_dict:
            raise ValueError("Please select one of the following values for set_\n\n{}".format(
                self.set_dict))
        else:
            self.set_ = set_

        self.template = [
                        "id",
                        "title",
                        "abstract",
                        "author",
                        "setSpec",
                        "categories",
                        "created",
                        "updated",
                        "comments",
                        "doi",
                        "datestamp",
                         ]

        # set the fields (columns) for the dataframe
        if fields == "everything":
            self.field = self.template
        elif isinstance(fields, (list, tuple, set)):
            self.field = fields
        else:
            raise TypeError("fields must be either a list, tuple, set, or 'everything'")

        # make a single row data frame for storing individual records
        self.single = pd.DataFrame(columns=self.field, index=[0])

        # make a dataframe for storing all the harvested papers
        self.pile = pd.DataFrame(columns=self.field)

    @staticmethod
    def days_back(i):
        """return the date for i days back
        i=0 : today
        i=1: yesterday
        """
        yesterday = (datetime.now() - timedelta(i))
        return yesterday.strftime('%Y-%m-%d')

    def get_file_name(self):
        """return the default file name for the pile"""
        fname = f"set={self.set_}-from={self.from_}-to={self.to_}.csv"
        return fname

    @staticmethod
    def check_date_format(date):
        """check the format of the date to ensure its validity"""
        try:
            date = parsedate(date)
            date = date.strftime("%Y-%m-%d")
            return date
        except ValueError:
            print("the input date must be in YYYY-mm-dd format")
            raise

    def process(self):
        """post process some of the columns in the pile
        1) convert dates to datetime datatype
        2) count the number of authors
        3) split sub-categories into list of items
        """
        try:
            self.pile["created"] = pd.to_datetime(self.pile["created"])
            self.pile["updated"] = pd.to_datetime(self.pile["updated"])
            self.pile["datestamp"] = pd.to_datetime(self.pile["datestamp"])
            self.pile["n_authors"] = self.pile["author"].map(len)
            self.pile.categories.apply(lambda x: x.split(" "))
        except KeyError:
            pass

    def save_to_file(self, filename=None, mode="csv"):
        """save the paper pile into a file"""
        if filename is None:
            filename = self.get_file_name()
        if mode == "csv":
            self.pile.to_csv(filename)


class Skimmer:
    """
    Skimmer takes in a soup object and parses the text

    attributes
    ----------

    pot: list
        contains list of soups acquired after each 1000 request
    bowls: list
        contains list of individual records (bowl)
    paper: object
        an instance of the Paper class

    """
    def __init__(self,
                 paper,
                 ):

        self.pot = []
        self.bowls = []
        self.paper = paper

    def add_to_pot(self, soup):
        """add new soup (single xml file) to the pot (list of xml files)"""
        self.pot.append(soup)

    def scoop(self):
        """go through the pot and extract the records"""

        print(len(self.pot))
        for soup in self.pot:
            bowl = soup.find_all("record")

            self.bowls.append(bowl)

        # flatten the list of bowls
        # NOTE: all the initial bowls must be lists
        self.bowls = list(chain.from_iterable(self.bowls))
        print("bowls = {}".format(len(self.bowls)))

    @staticmethod
    def find(bowl, keyword):
        """find keyword in the soup bowl and return the relevant text"""
        keywords = bowl.find_all(keyword)
        return [item.get_text(" ", strip=True) for item in keywords]

    def skim(self, paper):
        """skim each bowl and extract only the relevant info for each paper
        record the info in the paper.pile"""

        # go through all the records
        for bowl in self.bowls:
            # go through all the requested fields in the paper object
            for column in paper.single.columns:
                # find the relevant info for the column in the record
                ingredient = self.find(bowl, column)
                # if the list is empty, convert it to NaN
                if len(ingredient) == 0:
                    ingredient = np.nan
                # set the fields (columns) for the dataframe
                elif len(ingredient) == 1:
                    ingredient = ingredient[0]

                # add the extracted info to the paper object
                paper.single.at[0, column] = ingredient

            # add the record info to the paper.pile
            paper.pile = paper.pile.append(paper.single, ignore_index=True)


class arXiv:
    """Scraper object downloads and decodes the records based on user request

    attributes
    ----------

    from_: string [YYYY-mm-dd]
        The start date for the harvest
    to_: string [YYYY-mm-dd]
        The end date for the harvest
    set_: string
        The main category to harvest
    url_dict: dictionary
        Contains parameters of the request url
    url: string
        url address
    token: None or string
        Saves the resumption token for requests with more than 1000 items
    resume_url: string
        url address for resuming requests with more than a 1000 items
    paper: object
        Instance of the Paper class
    skimmer: object
        Instance of the Skimmer class


    methods
    -------

    """

    BASE_URL = "http://export.arxiv.org/oai2?verb=ListRecords&"
    METADATAPREFIX = "arXiv"  # link to the arXiv halp page

    def __init__(self,
                 paper,
                 n=1,):

        self.from_ = paper.from_
        self.to_ = paper.to_
        self.set_ = paper.set_

        self.url_dict = {
            "base_url": self.BASE_URL,
            "metadataprefix": self.METADATAPREFIX,
            "from": self.from_,
            "to": self.to_,
            "set": self.set_,
            }

        self.url = "{base_url}from={from}&until={to}&metadataPrefix={metadataprefix}&set={" \
              "set}".format(**self.url_dict)

        print(self.url)

        self.token = None
        self.resume_url = self.url
        self.paper = paper
        self.skimmer = Skimmer(self.paper)

        self.harvest()
        self.skimmer.scoop()
        self.skimmer.skim(self.paper)

    def harvest(self):
        """
        Request records from url and pass through the skimmer object
        """
        print(f"requesting records from {self.from_} to {self.to_} in the {self.set_} category")
        while True:
            print(f"request url: {self.resume_url}")

            # get contents from resume_url
            response = requests.get(self.resume_url)
            print(f"response ok? : {response.ok}\n")

            # if no errors occurred, make soup with the record
            if response.ok:
                soup = self.make_soup(response)

                #TODO: search soup for error

                #TODO: apply pool on pandas dataframe?

                self.skimmer.add_to_pot(soup)

                # check the soup for a resumption token
                token = self.check_for_token(soup)
                # if tokens found, add then to BASE_URL and continue downloading records
                if token is not None:
                    self.resume_url = self.BASE_URL + "resumptionToken={}".format(token.text)
                else:
                    break
            # if received a 503 error, sleep for the amount indicated in the error
            elif response.status_code == 503:
                self.sleep_off_503(response.text)
            # if response was not "ok", print the message
            else:
                print(f"response.text = {response.text}")
                break

    @staticmethod
    def make_soup(response):
        """make beautifulsoup with the url response"""
        soup = BeautifulSoup(response.text, "xml")

        return soup

    @staticmethod
    def check_for_token(soup):
        """check the soup for resumption token"""
        try:
            token = soup.find('ListRecords').find("resumptionToken")
            print("token=", token.text)
            return token
        except AttributeError:
            print("No resumption tokens found...\n")

            return None

    @staticmethod
    def sleep_off_503(text):
        """sleep for the amount indicated in the text"""
        error_pattern = "[\s\S]*?Retry after (\d+) seconds[\s\S]*"
        print(text)
        t = re.match(error_pattern, text)
        t = int(t[1])
        print(f"sleeping for {t} seconds...\n")
        sleep(t)


# class Soup(BeautifulSoup):
#
#     def find_resumptionToken(self):
#         return
if __name__ == "__main__":
    paper = Paper(set_="physics:astro-ph")
    arXiv(paper)
    print(paper.pile)
    paper.save_to_file()