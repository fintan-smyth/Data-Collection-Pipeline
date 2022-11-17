from data_collection import scraper
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import boto3
import json
import os
import random
import requests
import shutil
import unittest

class scraperTestCase(unittest.TestCase):
    def setUp(self):
        self.scrapetest = scraper.scraper()
    
    def tearDown(self):
        self.scrapetest.driver.quit()

    # @unittest.skip
    def test_accept_cookies(self):
        accepted = self.scrapetest.accept_cookies()
        self.assertTrue(accepted)

    # @unittest.skip
    def test_get_film_links_from_single_page(self):
        self.scrapetest.accept_cookies()
        link_list = self.scrapetest.get_film_links_from_single_page()
        n_links = len(link_list)
        self.assertEqual(n_links, 72)
    
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
        self.assertTrue(len(film_data) == 14)
        for key in film_data:
            if type(film_data[key]) == str:
                self.assertTrue(len(film_data[key]) > 0)
    
    # @unittest.skip
    def test_check_if_link_already_scraped(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'letterboxd-db.c4dnzzretdoh.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'password'
        PORT = 5432
        DATABASE = 'postgres'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

        self.assertFalse(self.scrapetest.check_if_link_already_scraped('https://letterboxd.com/film/testfilm/'))
        self.assertTrue(self.scrapetest.check_if_link_already_scraped('https://letterboxd.com/film/parasite-2019/'))

    # @unittest.skip
    def test_data_storage_options(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'letterboxd-db.c4dnzzretdoh.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'password'
        PORT = 5432
        DATABASE = 'postgres'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('letterboxd-data-bucket')

        film_data_dic = {"friendly_id": "testfilm", 
                         "uuid": "1234-5678-9012-3456", 
                         "title": "Test Film", 
                         "year": 1998, 
                         "runtime": 420, 
                         "rating": 5.0, 
                         "watches": 10000000, 
                         "lists": 999999, 
                         "likes": 3141592, 
                         "director": "Fintan Smyth", 
                         "top_250_position": 1, 
                         "description": 'Test description', 
                         "poster_link": "https://a.ltrbxd.com/resized/film-poster/2/4/0/3/4/4/240344-la-la-land-0-500-0-750-crop.jpg?v=053670ff84", 
                         "data_obtained_time": datetime.now()}
        
        self.scrapetest.accept_cookies()

        friendly_id = film_data_dic['friendly_id']
        film_data_dic['data_obtained_time'] = str(film_data_dic['data_obtained_time'])
        try:
            os.mkdir(f'raw_data/{friendly_id}')
        except:
            pass      
        with open(f'raw_data/{friendly_id}/data.json', 'w') as film_data_dic_file:
            json.dump(film_data_dic, film_data_dic_file)      
        image_request = requests.get(film_data_dic['poster_link'])
        try:
            os.mkdir(f'raw_data/{friendly_id}/images')
        except:
            pass
        with open(f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg', 'wb') as image_file:
            image_file.write(image_request.content)

        print('\n\n SELECT YES TO ALL OPTIONS FOR TESTING \n\n')
        self.scrapetest.data_storage_options_prompt()
        self.scrapetest.implement_data_storage_options(film_data_dic)
        objects = []
        for file in bucket.objects.filter(Prefix='raw_data/testfilm/'):
            objects.append(file.key)
        self.assertTrue(len(objects) == 2)
        bucket.objects.filter(Prefix='raw_data/testfilm/').delete()
        shutil.rmtree('raw_data/testfilm', ignore_errors=True)
        self.assertTrue(os.access('film_data.csv', os.F_OK))
        csv_df = pd.read_csv('film_data.csv', index_col=0)
        if 'testfilm' in csv_df.index:
            csv_df = csv_df.drop(index='testfilm')
            csv_df.to_csv('film_data.csv')
        else:
            raise Exception('Failed to save data to .csv')
        rds_df = pd.read_sql_table('film_data', engine).set_index('friendly_id')
        if 'testfilm' in rds_df.index:
            engine.execute("""DELETE FROM film_data WHERE friendly_id = 'testfilm'""")            
        else:
            raise Exception('Failed to upload data to RDS')

unittest.main(argv=[''], verbosity=1, exit=False)