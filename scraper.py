from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import json
import os
import requests
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

    def __scrape_text_data(self, film_data_dic: dict):

        delay = 10
        driver = self.driver

        self.__scrape_text_element(film_data_dic, 'Title', '//h1[@class="headline-1 js-widont prettify"]')
        self.__scrape_text_element(film_data_dic, 'Year', '//a[starts-with(@href,"/films/year/")]')
        self.__scrape_text_element(film_data_dic, 'Runtime', '//p[@class="text-link text-footer"]')
        self.__scrape_text_element(film_data_dic, 'Rating', '//a[starts-with(@class,"tooltip display-rating")]')
        self.__scrape_text_element(film_data_dic, 'Watches', '//a[@class="has-icon icon-watched icon-16 tooltip"]')
        self.__scrape_text_element(film_data_dic, 'Lists', '//a[@class="has-icon icon-list icon-16 tooltip"]')
        self.__scrape_text_element(film_data_dic, 'Likes', '//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]')
        
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
            film_data_dic['Top 250 Position'] = 'N/A'

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[@class="review body-text -prose -hero prettify"]')))
        try:
            more_button = driver.find_element(by=By.XPATH, value='//span[@class="condense_control condense_control_more"]')
            more_button.click()
            description = driver.find_element(by=By.XPATH, value='//div[@class="truncate condenseable"]').text.split('×')[0]   
        except:
            description = driver.find_element(by = By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
        film_data_dic['Description'] = description
        print(f'Description: {description}')
    
    def accept_cookies(self):
        '''
        Closes the 'accept cookies' pop-up.
        '''
        delay = 10
        try:
            WebDriverWait(self.driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[@class="banner_consent--2qj6F"][2]')))
            print('Accept cookies button ready...')
            accept_cookies_button = self.driver.find_element(by=By.XPATH, value='//*[@class="banner_consent--2qj6F"][2]')
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
        self.__scrape_image_data(film_data_dic)
        print('\n')
        time.sleep(1)

        return film_data_dic

    def store_scraped_data(self, film_data_dic: dict):
        '''
        Stores scraped data for single film locally.

        Parameters
        ----------
        film_data_dic: dict
            A dictionary containing all scraped data for a single film.
        '''
        friendly_id = film_data_dic['friendly_id']
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


if __name__ == "__main__":
    lbox_scrape = scraper(start_page = 1)
    lbox_scrape.accept_cookies()
    # link_list = lbox_scrape.get_film_links(pages = 1)
    # # link_list = ['https://letterboxd.com/film/spider-man-into-the-spider-verse/', 'https://letterboxd.com/film/ratatouille/', 'https://letterboxd.com/film/lady-bird/', 'https://letterboxd.com/film/dune-2021/', 'https://letterboxd.com/film/the-grand-budapest-hotel/', 'https://letterboxd.com/film/once-upon-a-time-in-hollywood/', 'https://letterboxd.com/film/la-la-land/', 'https://letterboxd.com/film/whiplash-2014/', 'https://letterboxd.com/film/avengers-infinity-war/', 'https://letterboxd.com/film/the-wolf-of-wall-street/', 'https://letterboxd.com/film/everything-everywhere-all-at-once/', 'https://letterboxd.com/film/the-shining/']
    # for links in link_list:
    #     try:
    #         film_data_dic = lbox_scrape.scrape_data_from_film_entry(links)
    #         # lbox_scrape.store_scraped_data(film_data_dic) 
    #     except:
    #         print('\nAborted\n')
    #         break
    lbox_scrape.driver.quit()
    # help(scraper)














