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
    A webscraper used to retrieve film data from the 'Popular' section of letterboxd.com.

    Parameters
    ----------
    start_page: int
        The number of the first page in the 'popular' section that will be scraped for links to film entries.
    
    Attributes
    ----------
    driver: selenium webdriver instance
        A driven firefox browser that will be used to navigate letterboxd.com and retrieve data.
    start_page: int
        The number of the first page in the 'popular' section that the scraper will be scraped for links to film entries (equal to start_page parameter).
    start_url: str
        The URL of the page the driver will navigate to upon initialisation.
    '''
    def __init__(self, start_page: int = 1):
        '''
        See help(scraper) for accurate signature.
        '''
        self.driver = webdriver.Firefox()
        self.film_data_dic_list = []
        self.start_page = start_page
        self.start_url = f"https://letterboxd.com/films/popular/page/{self.start_page}"
        self.driver.get(self.start_url)

    def __get_film_links_from_single_page(self) -> list:
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

    def __scrape_image_data(self, film_data_dic: dict):

        delay = 10
        driver = self.driver
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[starts-with(@class,"react-component poster")]')))
        poster_container = driver.find_element(by=By.XPATH, value='//div[starts-with(@class,"react-component poster")]')
        img_tag = poster_container.find_element(by=By.TAG_NAME, value = 'img')
        poster_link = img_tag.get_attribute('src')
        film_data_dic['Poster'] = poster_link
        print(f'Poster link: {poster_link}')

    def __scrape_text_element(self, film_data_dic: dict, element: str, xpath: str):
        
        delay = 10
        driver = self.driver

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
        scraped_text = driver.find_element(by=By.XPATH, value=xpath).text.partition('  ')[0]
        film_data_dic[element] = scraped_text
        print(f'{element}: {scraped_text}')

    def __scrape_film_stat_element(self, film_data_dic: dict, element: str, xpath: str):

        delay = 10
        driver = self.driver

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
        stat_container = driver.find_element(by=By.XPATH, value=xpath)
        stat = stat_container.get_attribute('data-original-title').split()[2]
        film_data_dic[f'{element}'] = stat
        print(f'{element}: {stat}')

    def __scrape_text_data(self, film_data_dic: dict):

        delay = 10
        driver = self.driver

        self.__scrape_text_element(film_data_dic, 'Title', '//h1[@class="headline-1 js-widont prettify"]')
        self.__scrape_text_element(film_data_dic, 'Year', '//a[starts-with(@href,"/films/year/")]')
        self.__scrape_text_element(film_data_dic, 'Runtime', '//p[@class="text-link text-footer"]')
        self.__scrape_text_element(film_data_dic, 'Rating', '//a[starts-with(@class,"tooltip display-rating")]')

        self.__scrape_film_stat_element(film_data_dic, 'Watches', '//a[@class="has-icon icon-watched icon-16 tooltip"]')
        self.__scrape_film_stat_element(film_data_dic, 'Lists', '//a[@class="has-icon icon-list icon-16 tooltip"]')
        self.__scrape_film_stat_element(film_data_dic, 'Likes', '//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]')

        # WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[@class="has-icon icon-watched icon-16 tooltip"]')))
        # watches_container = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-watched icon-16 tooltip"]')
        # watches = watches_container.get_attribute('data-original-title').split()[2]
        # film_data_dic['Watches'] = watches
        # print(f'Watches: {watches}')
        
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@href,"/director/")]')))
        director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]').text
        try:
            next_director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]/following-sibling::a').text
            directors = [director, next_director]
            film_data_dic['Director'] = directors
            print(f'Director: {directors}')
        except:
            film_data_dic['Director'] = director
            print(f'Director: {director}')
        
        try:
            top_250_pos = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-top250 icon-16 tooltip"]').text
            film_data_dic['Top 250 Position'] = top_250_pos
            print(f'Top 250 position: {top_250_pos}')
        except:
            film_data_dic['Top 250 Position'] = np.nan
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[@class="review body-text -prose -hero prettify"]')))
        try:
            more_button = driver.find_element(by=By.XPATH, value='//span[@class="condense_control condense_control_more"]')
            more_button.click()
            description = driver.find_element(by=By.XPATH, value='//div[@class="truncate condenseable"]').text.split('×')[0]   
        except:
            description = driver.find_element(by=By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
        film_data_dic['Description'] = description
        print(f'Description: {description}')
    
    def __clean_scraped_data(self, film_data_dic: dict) -> dict:
        film_data_dic['Year'] = int(film_data_dic['Year'])
        film_data_dic['Runtime'] = int(film_data_dic['Runtime'].split()[0])
        film_data_dic['Rating'] = float(film_data_dic['Rating'])
        watches = film_data_dic['Watches'].replace(',', '')
        film_data_dic['Watches'] = int(watches)
        lists = film_data_dic['Lists'].replace(',', '')
        film_data_dic['Lists'] = int(lists)
        likes = film_data_dic['Likes'].replace(',', '')
        film_data_dic['Likes'] = int(likes)
        director = film_data_dic['Director']
        if len(director) == 2:
            film_data_dic['Director'] = f'{director[0]}, {director[1]}'
        elif len(director) == 3:
            film_data_dic['Director'] = f'{director[0]}, {director[1]}, {director[2]}'
        else:
            pass
        try:
            film_data_dic['Top 250 Position'] = int(film_data_dic['Top 250 Position'])
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
        image_request = requests.get(film_data_dic['Poster'])
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

        s3_client = boto3.client('s3')
        s3_client.upload_file(f'raw_data/{friendly_id}/data.json', 'letterboxd-data-bucket', f'raw_data/{friendly_id}/data.json')
        s3_client.upload_file(f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg', 'letterboxd-data-bucket', f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg')

    def __store_tabular_data_rds(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = 'letterboxd-db.c4dnzzretdoh.eu-west-2.rds.amazonaws.com'
        USER = 'postgres'
        PASSWORD = 'password'
        PORT = 5432
        DATABASE = 'postgres'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")

        film_data_df = pd.DataFrame.from_dict(self.film_data_dic_list).set_index('friendly_id')
        film_data_df['Top 250 Position'] = film_data_df['Top 250 Position'].astype('Int64')
        film_data_df.to_sql('film_data', engine, if_exists='append')
        # print(film_data_df.info(verbose=True))
        # df = pd.read_sql_table('film_data', engine)
        # print(df)

    def __save_tabular_data_csv(self):
        film_data_df = pd.DataFrame.from_dict(self.film_data_dic_list).set_index('friendly_id')
        film_data_df['Top 250 Position'] = film_data_df['Top 250 Position'].astype('Int64')
        output_path='film_data.csv'
        film_data_df.to_csv(output_path, mode='a', header=not os.path.exists(output_path))

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
    
    def get_film_links(self, pages: int) -> list:
        '''
        Scrapes the links to film entries from a set number of pages, starting from the 'start_page'.

        Parameters
        ----------
        pages: int
            The number of pages from which to scrape links to film entries.
        
        Returns
        -------
        link_list: list
            A list of links to film entries in the letterboxd.com system.
        '''
        link_list = []
        link_list.extend(self.__get_film_links_from_single_page())
        
        next_page = self.start_page + 1
        for i in range(pages-1):
            time.sleep(1)
            next_page_url = f'https://letterboxd.com/films/popular/size/small/page/{next_page}/'
            self.driver.get(next_page_url)
            print(f'Page {next_page} loaded.')
            next_page += 1
            link_list.extend(self.__get_film_links_from_single_page())
        
        return link_list

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
            print(f'friendly_id: {friendly_id}')
            film_data_dic['friendly_id'] = friendly_id

            film_uuid = uuid.uuid4()
            print(f'UUID: {film_uuid}')
            film_data_dic['UUID'] = str(film_uuid)

            self.__scrape_text_data(film_data_dic)
            if film_data_dic['Description'] != '':
                break
            print("\nFailed to scrape 'Description'. Reloading link...\n")
        self.__scrape_image_data(film_data_dic)

        timestamp = datetime.now()
        print(f'data_obtained_time: {timestamp}')
        film_data_dic['data_obtained_time'] = timestamp


        film_data_dic = self.__clean_scraped_data(film_data_dic)
        self.film_data_dic_list.append(film_data_dic)
        print('\n')
        time.sleep(1)

        return film_data_dic

    def store_raw_scraped_data(self, film_data_dic: dict):
        '''
        Saves the scraped raw data for a single film locally and uploads it to an s3 bucket.

        Parameters
        ----------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        self.__store_raw_data_local(film_data_dic)
        self.__store_raw_data_s3(film_data_dic)

    def data_storage_options_prompt(self):
        '''
        Prompts the user for how they would like to store their data.
        '''
        while True:
            rds_reply = input('\nUpload tabular data to AWS RDS? (Y/N)').lower().strip()
            if rds_reply[0] == 'y':
                self.__store_tabular_data_rds()
                print('\nTabular data uploaded to RDS')
                break
            elif rds_reply[0] == 'n':
                break
            else:
                print('\nPlease choose yes or no.')
        while True:
            csv_reply = input('\nSave tabular data locally as .csv? (Y/N)').lower().strip()
            if csv_reply[0] == 'y':
                self.__save_tabular_data_csv()
                print('\nTabular data saved to film_data.csv')
                break
            elif csv_reply[0] == 'n':
                break
            else:
                print('\nPlease choose yes or no.')
        while True:
            raw_data_reply = input('\nKeep local copy of raw data? (Y/N)').lower().strip()
            if raw_data_reply[0] == 'y':
                print("\nRaw data saved in 'raw_data' folder")
                break
            elif raw_data_reply[0] == 'n':
                shutil.rmtree('raw_data', ignore_errors=True)
                print('\nLocal copy of raw data removed')
                break
            else:
                print('\nPlease choose yes or no.')

        
if __name__ == "__main__":
    lbox_scraper = scraper(start_page = 1)
    lbox_scraper.accept_cookies()
    # link_list = lbox_scraper.get_film_links(pages = 1)
    # link_list = ['https://letterboxd.com/film/spider-man-into-the-spider-verse/', 'https://letterboxd.com/film/ratatouille/', 'https://letterboxd.com/film/lady-bird/', 'https://letterboxd.com/film/dune-2021/', 'https://letterboxd.com/film/the-grand-budapest-hotel/', 'https://letterboxd.com/film/once-upon-a-time-in-hollywood/', 'https://letterboxd.com/film/la-la-land/', 'https://letterboxd.com/film/whiplash-2014/', 'https://letterboxd.com/film/avengers-infinity-war/', 'https://letterboxd.com/film/the-wolf-of-wall-street/', 'https://letterboxd.com/film/everything-everywhere-all-at-once/', 'https://letterboxd.com/film/the-shining/']
    link_list = ['https://letterboxd.com/film/ratatouille/', 'https://letterboxd.com/film/avengers-infinity-war/']
    for link in link_list:
        try:
            film_data_dic = lbox_scraper.scrape_data_from_film_entry(link)
            lbox_scraper.store_raw_scraped_data(film_data_dic)
        except:
            print('\nAborted\n')
            break
    
    lbox_scraper.driver.quit()
    lbox_scraper.data_storage_options_prompt()
















