from scraper import scraper
from hypothesis import given
import hypothesis.strategies as st
import os
import random
import shutil
import unittest

class scraperTestCase(unittest.TestCase):
    def setUp(self):
        self.scrapetest = scraper()

    # @unittest.skip
    def test_accept_cookies(self):
        accepted = self.scrapetest.accept_cookies()
        self.assertTrue(accepted)


    # @unittest.skip
    def test_get_film_links(self):
        pages = 5
        self.scrapetest.accept_cookies()
        link_list = self.scrapetest.get_film_links(pages)
        n_links = len(link_list)
        self.assertEqual(n_links, pages*72)
    
    # @unittest.skip
    def test_scrape_data_from_film_entry(self):
        self.scrapetest.accept_cookies()
        link_list = ['https://letterboxd.com/film/spider-man-into-the-spider-verse/', 
                     'https://letterboxd.com/film/ratatouille/', 
                     'https://letterboxd.com/film/lady-bird/', 
                     'https://letterboxd.com/film/dune-2021/', 
                     'https://letterboxd.com/film/the-grand-budapest-hotel/', 
                     'https://letterboxd.com/film/once-upon-a-time-in-hollywood/', 
                     'https://letterboxd.com/film/la-la-land/', 
                     'https://letterboxd.com/film/whiplash-2014/', 
                     'https://letterboxd.com/film/avengers-infinity-war/', 
                     'https://letterboxd.com/film/the-wolf-of-wall-street/', 
                     'https://letterboxd.com/film/everything-everywhere-all-at-once/', 
                     'https://letterboxd.com/film/the-shining/']
        film_link = random.choice(link_list)
        film_data = self.scrapetest.scrape_data_from_film_entry(film_link)
        self.assertTrue(len(film_data) == 13)
        for key in film_data:
            self.assertTrue(len(film_data[key]) > 0)
    
    # @unittest.skip
    def test_store_scraped_data(self):
        self.scrapetest.accept_cookies()
        film_data_dic = {"friendly_id": "testfilm",
                         "UUID": "1234-5678-9012-3456", 
                         "Title": "Test Film", 
                         "Year": "1998", 
                         "Runtime": "420 mins", 
                         "Rating": "5.0", 
                         "Watches": "999K", 
                         "Lists": "100K", 
                         "Likes": "10M", 
                         "Director": "Fintan Smyth", 
                         "Top 250 Position": "1", 
                         "Description": "Test description",
                         "Poster": "https://a.ltrbxd.com/resized/sm/upload/dp/sq/oj/cg/hNjxJbejPHSmKVidHQ9ZHaC0Z7r-0-230-0-345-crop.jpg?v=f0ff67d1b9"}
        self.scrapetest.store_scraped_data(film_data_dic)
        self.assertTrue(os.access('raw_data/testfilm/data.json', os.F_OK))
        self.assertTrue(os.access('raw_data/testfilm/images/testfilm_poster.jpg', os.F_OK))
        shutil.rmtree('raw_data/testfilm')


    def tearDown(self):
        self.scrapetest.driver.quit()

unittest.main(argv=[''], verbosity=2, exit=False)