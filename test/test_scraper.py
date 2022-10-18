from data_collection import scraper
from sqlalchemy import create_engine, delete
import pandas as pd
import boto3
import os
import random
import shutil
import unittest

class scraperTestCase(unittest.TestCase):
    def setUp(self):
        self.scrapetest = scraper.scraper()

    @unittest.skip
    def test_accept_cookies(self):
        accepted = self.scrapetest.accept_cookies()
        self.assertTrue(accepted)

    @unittest.skip
    def test_get_film_links(self):
        pages = 3
        self.scrapetest.accept_cookies()
        link_list = self.scrapetest.get_film_links(pages)
        n_links = len(link_list)
        self.assertEqual(n_links, pages*72)
    
    @unittest.skip
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
        # film_link = random.choice(link_list)
        film_link = 'https://letterboxd.com/film/avengers-infinity-war/'
        film_data = self.scrapetest.scrape_data_from_film_entry(film_link)
        self.assertTrue(len(film_data) == 14)
        for key in film_data:
            self.assertTrue(len(film_data[key]) > 0)
    
    @unittest.skip
    def test_store_raw_scraped_data(self):
        self.scrapetest.accept_cookies()
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('letterboxd-data-bucket')
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
                         "Poster": "https://a.ltrbxd.com/resized/sm/upload/dp/sq/oj/cg/hNjxJbejPHSmKVidHQ9ZHaC0Z7r-0-230-0-345-crop.jpg?v=f0ff67d1b9",
                         "data_obtained_time": "2020-01-01 16:20:00"}
        self.scrapetest.store_raw_scraped_data(film_data_dic)
        self.assertTrue(os.access('raw_data/testfilm/data.json', os.F_OK))
        self.assertTrue(os.access('raw_data/testfilm/images/testfilm_poster.jpg', os.F_OK))
        objects = []
        for file in bucket.objects.filter(Prefix='raw_data/testfilm/'):
            objects.append(file.key)
        self.assertTrue(len(objects) == 2)
        bucket.objects.filter(Prefix='raw_data/testfilm/').delete()
        shutil.rmtree('raw_data/testfilm')

    # @unittest.skip
    def test_data_storage_options_prompt(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'letterboxd-db.c4dnzzretdoh.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'password'
        PORT = 5432
        DATABASE = 'postgres'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

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
                         "Poster": "https://a.ltrbxd.com/resized/sm/upload/dp/sq/oj/cg/hNjxJbejPHSmKVidHQ9ZHaC0Z7r-0-230-0-345-crop.jpg?v=f0ff67d1b9",
                         "data_obtained_time": "2020-01-01 16:20:00"}
        self.scrapetest.film_data_dic_list.append(film_data_dic)
        self.scrapetest.data_storage_options_prompt()
        self.assertTrue(os.access('film_data.csv', os.F_OK))
        csv_df = pd.read_csv('film_data.csv', index_col=0)
        csv_df = csv_df.drop(index='testfilm')
        csv_df.to_csv('film_data.csv')
        rds_df = pd.read_sql_table('film_data', engine)
        print(rds_df.loc[rds_df['friendly_id'] == 'testfilm'])
        # finish after next prereq content





    def tearDown(self):
        self.scrapetest.driver.quit()

unittest.main(argv=[''], verbosity=3, exit=False)