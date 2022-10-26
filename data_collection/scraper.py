from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import boto3
import json
import os
import requests
import shutil
import time
import uuid


class scraper:
    '''
    A webscraper used to retrieve film data from the 'Popular' section of letterboxd.com
    
    Attributes
    ----------
    driver: selenium webdriver instance
        A driven firefox browser that will be used to navigate letterboxd.com and retrieve data.
    engine: sqlalchemy database connection
        A connection to my AWS RDS database using the sqlalchemy package.
    film_data_dic_list: list of dict
        An empty list that will be populated with dictionaries containing film data.
    start_page: int
        The number of the first page in the 'popular' section that the scraper will be scraped for links to film entries (equal to start_page parameter).
    pages: int
        The number of pages in the 'popular' section that the scraper will obtain links to film entries from.
    s3_storage_bool: bool
        Boolean corresponding to whether data will be stored in an s3 bucket.
    keep_raw_data_bool: bool
        Boolean corresponding to whether a local copy of raw data will be kept.
    rds_bool: bool
        Boolean corresponding to whether tabular data will be stored in an RDS database.
    csv_bool: bool
        Boolean corresponding to whether tabular data will be saved locally as a .csv.
    start_url: str
        The URL of the page the driver will navigate to upon initialisation.
    '''
    def __init__(self):
        '''
        See help(scraper) for accurate signature.
        '''
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("window-size=1920,1080")
        self.driver = webdriver.Firefox(options=options)
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'letterboxd-db.c4dnzzretdoh.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'password'
        PORT = 5432
        DATABASE = 'postgres'
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        
        while True:
            start_page_prompt = int(input('Please choose a starting page: '))
            if start_page_prompt > 0:
                self.start_page = start_page_prompt
                break
            else:
                print('Please choose a positive integer...')
        
        while True:
            pages_prompt = int(input('Please choose a number of pages to scrape: '))
            if pages_prompt > 0:
                self.pages = pages_prompt
                break
            else:
                print('Please choose a positive integer...')
        self.s3_storage_bool = True
        self.keep_raw_data_bool = True
        self.rds_bool = True
        self.csv_bool = False
        self.start_url = f"https://letterboxd.com/films/popular/page/{self.start_page}"
        self.driver.get(self.start_url)

    def __scrape_image_data(self, film_data_dic: dict):

        delay = 10
        driver = self.driver
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[starts-with(@class,"react-component poster")]')))
        poster_container = driver.find_element(by=By.XPATH, value='//div[starts-with(@class,"react-component poster")]')
        img_tag = poster_container.find_element(by=By.TAG_NAME, value = 'img')
        poster_link = img_tag.get_attribute('src')
        film_data_dic['poster_link'] = poster_link
        # print(f'Poster link: {poster_link}')

    def __scrape_text_element(self, film_data_dic: dict, element: str, xpath: str):
        
        delay = 10
        driver = self.driver

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
        scraped_text = driver.find_element(by=By.XPATH, value=xpath).text.partition('  ')[0]
        film_data_dic[element] = scraped_text
        # print(f'{element}: {scraped_text}')

    def __scrape_film_stat_element(self, film_data_dic: dict, element: str, xpath: str):

        delay = 10
        driver = self.driver

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
        stat_container = driver.find_element(by=By.XPATH, value=xpath)
        stat = stat_container.get_attribute('data-original-title').split()[2]
        film_data_dic[f'{element}'] = stat
        # print(f'{element}: {stat}')

    def __scrape_all_text_data(self, film_data_dic: dict):

        delay = 10
        driver = self.driver

        self.__scrape_text_element(film_data_dic, 'title', '//h1[@class="headline-1 js-widont prettify"]')
        self.__scrape_text_element(film_data_dic, 'year', '//a[starts-with(@href,"/films/year/")]')
        self.__scrape_text_element(film_data_dic, 'runtime', '//p[@class="text-link text-footer"]')
        self.__scrape_text_element(film_data_dic, 'rating', '//a[starts-with(@class,"tooltip display-rating")]')

        self.__scrape_film_stat_element(film_data_dic, 'watches', '//a[@class="has-icon icon-watched icon-16 tooltip"]')
        self.__scrape_film_stat_element(film_data_dic, 'lists', '//a[@class="has-icon icon-list icon-16 tooltip"]')
        self.__scrape_film_stat_element(film_data_dic, 'likes', '//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]')

        
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@href,"/director/")]')))
        director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]').text
        try:
            next_director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]/following-sibling::a').text
            directors = [director, next_director]
            film_data_dic['director'] = directors
            # print(f'Director: {directors}')
        except:
            film_data_dic['director'] = director
            # print(f'Director: {director}')
        
        try:
            top_250_pos = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-top250 icon-16 tooltip"]').text
            film_data_dic['top_250_position'] = top_250_pos
            # print(f'Top 250 position: {top_250_pos}')
        except:
            film_data_dic['top_250_position'] = np.nan
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[@class="review body-text -prose -hero prettify"]')))
        try:
            more_button = driver.find_element(by=By.XPATH, value='//span[@class="condense_control condense_control_more"]')
            more_button.click()
            description = driver.find_element(by=By.XPATH, value='//div[@class="truncate condenseable"]').text.split('×')[0]   
        except:
            description = driver.find_element(by=By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
        film_data_dic['description'] = description
        # print(f'Description: {description}')
    
    def __clean_scraped_data(self, film_data_dic: dict) -> dict:
        film_data_dic['year'] = int(film_data_dic['year'])
        film_data_dic['runtime'] = int(film_data_dic['runtime'].split()[0].replace(',', ''))
        film_data_dic['rating'] = float(film_data_dic['rating'])
        watches = film_data_dic['watches'].replace(',', '')
        film_data_dic['watches'] = int(watches)
        lists = film_data_dic['lists'].replace(',', '')
        film_data_dic['lists'] = int(lists)
        likes = film_data_dic['likes'].replace(',', '')
        film_data_dic['likes'] = int(likes)
        director = film_data_dic['director']
        if len(director) == 2:
            film_data_dic['director'] = f'{director[0]}, {director[1]}'
        elif len(director) == 3:
            film_data_dic['director'] = f'{director[0]}, {director[1]}, {director[2]}'
        else:
            pass
        try:
            film_data_dic['top_250_position'] = int(film_data_dic['top_250_position'])
        except:
            pass
        return film_data_dic

    def __store_raw_data_local(self, film_data_dic: dict):
        '''
        Stores scraped data for single film locally.
        Parameters
        ----------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        friendly_id = film_data_dic['friendly_id']
        film_data_dic['data_obtained_time'] = str(film_data_dic['data_obtained_time'])
        try:
            os.mkdir('raw_data')
        except:
            pass
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

    def __store_raw_data_s3(self, film_data_dic: dict):
        '''
        Stores scraped data for single film in an s3 bucket.
        Parameters
        ----------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        friendly_id = film_data_dic['friendly_id']

        s3_client = boto3.client('s3', 
                                aws_access_key_id='AKIAWQVNI7VAFIWE6GHP',
                                aws_secret_access_key= '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')
        s3_client.upload_file(f'raw_data/{friendly_id}/data.json', 'letterboxd-data-bucket', f'raw_data/{friendly_id}/data.json')
        s3_client.upload_file(f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg', 'letterboxd-data-bucket', f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg')

    def __store_tabular_data_rds(self, film_data_dic: dict):
        film_data_df = pd.DataFrame([film_data_dic]).set_index('friendly_id')
        film_data_df['top_250_position'] = film_data_df['top_250_position'].astype('Int64')
        film_data_df.to_sql('film_data', self.engine, if_exists='append')

    def __remove_local_raw_data(self):
        shutil.rmtree('raw_data', ignore_errors=True)
        
    def __save_tabular_data_csv(self, film_data_dic: dict):
        film_data_df = pd.DataFrame([film_data_dic]).set_index('friendly_id')
        film_data_df['top_250_position'] = film_data_df['top_250_position'].astype('Int64')
        output_path='film_data.csv'
        film_data_df.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
        print('\nTabular data saved to to film_data.csv')

    def accept_cookies(self):
        '''
        Closes the 'accept cookies' pop-up.
        '''
        delay = 10
        try:
            WebDriverWait(self.driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[@class="fc-button fc-cta-consent fc-primary-button"]')))
            print('Accept cookies button ready...')
            accept_cookies_button = self.driver.find_element(by=By.XPATH, value='//*[@class="fc-button fc-cta-consent fc-primary-button"]')
            accept_cookies_button.click()
            print('Cookies accepted.\n')
            return True
        except TimeoutException:
            print('Cookies button did not load in time.\n')
            return False
        time.sleep(1)
    
    def get_film_links_from_single_page(self) -> list:
        delay = 10
        WebDriverWait(self.driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[@class="poster-list -p70 -grid"]/li')))
        print('Poster list ready...')
        film_container = self.driver.find_element(by=By.XPATH, value='//*[@class="poster-list -p70 -grid"]')
        film_list = film_container.find_elements(by=By.XPATH, value='./li')
        link_list = []

        for film in film_list:
            WebDriverWait(film, delay).until(EC.presence_of_element_located((By.TAG_NAME, 'a')))
            a_tag = film.find_element(By.TAG_NAME, 'a')
            link = a_tag.get_attribute('href')
            link_list.append(link)
        print('Links scraped.\n')
        return link_list

    def check_if_link_already_scraped(self, link: str) -> bool:
        '''
        Checks if there is already data from this link in the RDS database and returns a corresponding boolean.
        Returns True if there is already an entry with the same 'friendly_id' in the RDS database. Returns False if there is not.
        Parameters
        ----------
        link: str
            The link to a film entry on letterboxd.com.
        
        Returns
        -------
        bool
            A boolean corresponding to whether the link has already been scraped.
        '''
        link_id = link.split('/')[4]
        statement = f"""SELECT friendly_id FROM film_data WHERE friendly_id = '{link_id}'"""
        if len(self.engine.execute(statement).fetchall()) == 0:
            return False
        else:
            return True
        
    def scrape_data_from_film_entry(self, link: str) -> dict:
        '''
        Scrapes all chosen data from the page of a film entry and stores it in a dictionary.
        Parameters
        ----------
        link: str
            The link to a film entry on letterboxd.com.
        
        Returns
        -------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        while True:
            film_data_dic = {}
            driver = self.driver
            driver.get(link)

            friendly_id = link.split('/')[4]
            print(f'Scraping data for {friendly_id}...')
            film_data_dic['friendly_id'] = friendly_id

            film_uuid = uuid.uuid4()
            # print(f'UUID: {film_uuid}')
            film_data_dic['uuid'] = str(film_uuid)

            self.__scrape_all_text_data(film_data_dic)
            if film_data_dic['description'] != '':
                break
            print("Failed to scrape 'description'. Reloading link...")
        self.__scrape_image_data(film_data_dic)

        timestamp = datetime.now()
        # print(f'data_obtained_time: {timestamp}')
        film_data_dic['data_obtained_time'] = timestamp


        film_data_dic = self.__clean_scraped_data(film_data_dic)
        # self.film_data_dic_list.append(film_data_dic)
        # print('\n')
        time.sleep(1)

        return film_data_dic

    def data_storage_options_prompt(self):
        '''
        Prompts the user for how they would like to store their data.
        '''
        while True:
            s3_prompt = input('Store scraped raw data in s3 bucket? (Y/N)').lower().strip()
            if s3_prompt[0] == 'n':
                self.s3_storage_bool = False
                break
            elif s3_prompt[0] == 'y':
                self.s3_storage_bool = True
                break
            else:
                print('Please choose yes or no...')

        while True:
            keep_raw_prompt = input('Keep local copy of raw data? (Y/N)').lower().strip()
            if keep_raw_prompt[0] == 'n':
                self.keep_raw_data_bool = False
                break
            elif keep_raw_prompt[0] == 'y':
                self.keep_raw_data_bool = True
                break
            else:
                print('Please choose yes or no...')

        while True:
            rds_prompt = input('Store tabular data in RDS database? (Y/N)').lower().strip()
            if rds_prompt[0] == 'n':
                self.rds_bool = False
                break
            elif rds_prompt[0] == 'y':
                self.rds_bool = True
                break
            else:
                print('Please choose yes or no...')

        while True:
            csv_prompt = input('Save tabular data as .csv? (Y/N)').lower().strip()
            if csv_prompt[0] == 'n':
                self.csv_bool = False
                break
            elif csv_prompt[0] == 'y':
                self.csv_bool = True
                break
            else:
                print('Please choose yes or no...')

    def implement_data_storage_options(self, film_data_dic: dict):
        '''
        Implements the data storage options chosen in the prompt.
        Parameters
        ----------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        self.__store_raw_data_local(film_data_dic)
        if self.s3_storage_bool == True:
            self.__store_raw_data_s3(film_data_dic)
        if self.keep_raw_data_bool == False:
            self.__remove_local_raw_data()
        if self.rds_bool == True:
            self.__store_tabular_data_rds(film_data_dic)
        if self.csv_bool == True:
            self.__save_tabular_data_csv(film_data_dic)
    
        
if __name__ == "__main__":
    lbox_scraper = scraper()
    lbox_scraper.data_storage_options_prompt()
    lbox_scraper.accept_cookies()
    next_page = lbox_scraper.start_page + 1
    pages = lbox_scraper.pages
    for i in range(pages):
        link_list = lbox_scraper.get_film_links_from_single_page()
        for link in link_list:
            if lbox_scraper.check_if_link_already_scraped(link) == True:
                link_id = link.split('/')[4]
                print(f'Data for {link_id} already exists. Skipping to next link...')
                continue
            film_data_dic = lbox_scraper.scrape_data_from_film_entry(link)
            lbox_scraper.implement_data_storage_options(film_data_dic)
        if next_page == lbox_scraper.start_page + pages:
            break
        next_page_url = f'https://letterboxd.com/films/popular/size/small/page/{next_page}/'
        lbox_scraper.driver.get(next_page_url)
        print(f'Page {next_page} loaded.')
        next_page += 1
        
    lbox_scraper.driver.quit()









