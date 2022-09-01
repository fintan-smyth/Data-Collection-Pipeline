from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import time
import uuid
import json
import os
import requests


class scraper:

    def __init__(self, start_page = 1):

        self.driver = webdriver.Firefox()
        self.start_page = start_page
        self.start_url = f"https://letterboxd.com/films/popular/page/{self.start_page}"
        # self.film_dic = {'friendly_id': [],'UUID': [], 'Title': [], 'Year': [], 'Director': [], 'Runtime': [], 'Poster': [], 'Rating': [], 'Watches': [], 'Lists': [], 'Likes': [], 'Top 250 Position': [], 'Description': []}
        self.driver.get(self.start_url)

    def accept_cookies(self):
        delay = 10
        try:
            WebDriverWait(self.driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[@class="banner_consent--2qj6F"][2]')))
            print('Accept cookies button ready...')
            accept_cookies_button = self.driver.find_element(by=By.XPATH, value='//*[@class="banner_consent--2qj6F"][2]')
            accept_cookies_button.click()
            print('Cookies accepted.\n')
        except TimeoutException:
            print('Cookies button did not load in time.\n')
        time.sleep(1)

    def get_film_links(self):
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

    def get_film_links_from_several_pages(self, pages):
        
        big_list = []
        big_list.extend(self.get_film_links())
        
        next_page = self.start_page + 1
        for i in range(pages-1):
            time.sleep(1)
            next_page_url = f'https://letterboxd.com/films/popular/size/small/page/{next_page}/'
            self.driver.get(next_page_url)
            print(f'Page {next_page} loaded.')
            next_page += 1
            big_list.extend(self.get_film_links())
        
        return big_list

    def scrape_image_data(self, film_dic):

        delay = 10
        driver = self.driver
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[starts-with(@class,"react-component poster")]')))
        poster_container = driver.find_element(by=By.XPATH, value='//div[starts-with(@class,"react-component poster")]')
        img_tag = poster_container.find_element(by=By.TAG_NAME, value = 'img')
        poster_link = img_tag.get_attribute('src')
        film_dic['Poster'] = poster_link
        print(f'Poster link: {poster_link}')

    def scrape_text_element(self, film_dic, element, xpath):
        
        delay = 10
        driver = self.driver

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, xpath)))
        scraped_text = driver.find_element(by=By.XPATH, value=xpath).text.partition('  ')[0]
        film_dic[element] = scraped_text
        print(f'{element}: {scraped_text}')

    def scrape_text_data(self, film_dic):

        delay = 10
        driver = self.driver

        self.scrape_text_element(film_dic, 'Title', '//h1[@class="headline-1 js-widont prettify"]')
        self.scrape_text_element(film_dic, 'Year', '//a[starts-with(@href,"/films/year/")]')
        self.scrape_text_element(film_dic, 'Runtime', '//p[@class="text-link text-footer"]')
        self.scrape_text_element(film_dic, 'Rating', '//a[starts-with(@class,"tooltip display-rating")]')
        self.scrape_text_element(film_dic, 'Watches', '//a[@class="has-icon icon-watched icon-16 tooltip"]')
        self.scrape_text_element(film_dic, 'Lists', '//a[@class="has-icon icon-list icon-16 tooltip"]')
        self.scrape_text_element(film_dic, 'Likes', '//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]')
        
        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@href,"/director/")]')))
        director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]').text
        try:
            next_director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]/following-sibling::a').text
            directors = [director, next_director]
            film_dic['Director'] = directors
            print(f'Director: {directors}')
        except:
            film_dic['Director'] = director
            print(f'Director: {director}')
        
        try:
            top_250_pos = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-top250 icon-16 tooltip"]').text
            film_dic['Top 250 Position'] = top_250_pos
            print(f'Top 250 position: {top_250_pos}')
        except:
            film_dic['Top 250 Position'] = 'N/A'

        WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[@class="review body-text -prose -hero prettify"]')))
        try:
            more_button = driver.find_element(by=By.XPATH, value='//span[@class="condense_control condense_control_more"]')
            more_button.click()
            description = driver.find_element(by=By.XPATH, value='//div[@class="truncate condenseable"]').text.split('×')[0]   
        except:
            description = driver.find_element(by = By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
        film_dic['Description'] = description
        print(f'Description: {description}')
      
    def scrape_all_page_data(self, link):
        
        film_dic = {}
        driver = self.driver
        driver.get(link)

        friendly_id = link.split('/')[4]
        print(f'friendly_id: {friendly_id}')
        film_dic['friendly_id'] = friendly_id

        film_uuid = uuid.uuid4()
        print(f'UUID: {film_uuid}')
        film_dic['UUID'] = str(film_uuid)

        self.scrape_text_data(film_dic)
        self.scrape_image_data(film_dic)
        print('\n')
        time.sleep(1)

        return film_dic

    def store_scraped_data(self, film_data):
        friendly_id = film_data['friendly_id']
        try:
            os.mkdir(f'raw_data')
        except:
            pass
        try:
            os.mkdir(f'raw_data/{friendly_id}')
        except:
            pass      
        with open(f'raw_data/{friendly_id}/data.json', 'w') as film_data_file:
            json.dump(film_data, film_data_file)      
        image_request = requests.get(film_data['Poster'])
        try:
            os.mkdir(f'raw_data/{friendly_id}/images')
        except:
            pass
        with open(f'raw_data/{friendly_id}/images/{friendly_id}_poster.jpg', 'wb') as image_file:
            image_file.write(image_request.content)


if __name__ == "__main__":
    lbox_scrape = scraper(start_page = 13)
    lbox_scrape.accept_cookies()
    link_list = lbox_scrape.get_film_links_from_several_pages(pages = 8)
    # link_list = ['https://letterboxd.com/film/spider-man-into-the-spider-verse/', 'https://letterboxd.com/film/ratatouille/', 'https://letterboxd.com/film/lady-bird/', 'https://letterboxd.com/film/dune-2021/', 'https://letterboxd.com/film/the-grand-budapest-hotel/', 'https://letterboxd.com/film/once-upon-a-time-in-hollywood/', 'https://letterboxd.com/film/la-la-land/', 'https://letterboxd.com/film/whiplash-2014/', 'https://letterboxd.com/film/avengers-infinity-war/', 'https://letterboxd.com/film/the-wolf-of-wall-street/', 'https://letterboxd.com/film/everything-everywhere-all-at-once/', 'https://letterboxd.com/film/the-shining/']
    for links in link_list:
        film_data = lbox_scrape.scrape_all_page_data(links)
        lbox_scrape.store_scraped_data(film_data)        
    lbox_scrape.driver.quit()














