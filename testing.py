from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import numpy as np
import pandas as pd


class scraper:

    def __init__(self):

        self.driver = webdriver.Firefox()
        # self.driver.set_page_load_timeout(10)
        self.URL = "https://letterboxd.com/films/popular/"
        self.film_dic = {'Title': [], 'Year': [], 'Director': [], 'Runtime': [], 'Rating': [], 'Watches': [], 'Lists': [], 'Likes': [], 'Top 250 Position': [], 'Description': []}
        self.driver.get(self.URL)
        time.sleep(2)


    def accept_cookies(self):
        
        try:
            accept_cookies_button = self.driver.find_element(by=By.XPATH, value='//*[@class="banner_consent--2qj6F"][2]')
            accept_cookies_button.click()
        except:
            pass
        return self.driver

    def close_ads(self):
        
        ad_container = self.driver.find_element(by=By.XPATH, value='//*[@id="pw-oop-bottom_rail"]')
        # driver.switch_to.element('pw-oop-bottom_rail')
        close_button = ad_container.find_element(by=By.XPATH, value='//*[starts-with(@style,"background")]')
        close_button.click()


    def get_links(self):
        
        film_container = self.driver.find_element(by=By.XPATH, value='//*[@class="poster-list -p70 -grid"]')
        film_list = film_container.find_elements(by=By.XPATH, value='./li')
        link_list = []
        time.sleep(2)

        for film in film_list:
            a_tag = film.find_element(By.TAG_NAME, 'a')
            link = a_tag.get_attribute('href')
            link_list.append(link)

        return link_list

    def get_links_from_several_pages(self, pages):
        
        big_list = []
        next_page = 2
        try:
            for i in range(pages):
                time.sleep(2)
                big_list.extend(self.get_links())
                next_page_url = f'https://letterboxd.com/films/popular/size/small/page/{next_page}/'
                self.driver.get(next_page_url)
                next_page += 1
        except:
            pass
        
        return big_list
    
    def scrape_info(self, links):

        for link in links:
            start = time.time()
            driver = self.driver
            driver.get(link)
            time.sleep(2)
            title = driver.find_element(by=By.XPATH, value='//h1[@class="headline-1 js-widont prettify"]').text
            self.film_dic['Title'].append(title)
            year = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/films/year/")]').text
            self.film_dic['Year'].append(year)
            director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]').text
            directors = [director]
            try:
                next_director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]/following-sibling::a').text
                directors.append(next_director)
            except:
                pass
            if len(directors) > 1:
                self.film_dic['Director'].append(directors)
            else:
                self.film_dic['Director'].append(director)
            length = driver.find_element(by=By.XPATH, value='//p[@class="text-link text-footer"]').text.partition('  ')[0]
            self.film_dic['Runtime'].append(length)
            avg_rating = driver.find_element(by=By.XPATH, value='//a[starts-with(@class,"tooltip display-rating")]').text
            self.film_dic['Rating'].append(avg_rating)
            n_watches = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-watched icon-16 tooltip"]').text
            self.film_dic['Watches'].append(n_watches)
            n_lists = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-list icon-16 tooltip"]').text
            self.film_dic['Lists'].append(n_lists)
            n_likes = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]').text
            self.film_dic['Likes'].append(n_likes)
            try:
                top_250_pos = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-top250 icon-16 tooltip"]').text
                self.film_dic['Top 250 Position'].append(top_250_pos)
            except:
                self.film_dic['Top 250 Position'].append('N/A')
            description = driver.find_element(by = By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
            self.film_dic['Description'].append(description)
            time.sleep(1)
        return self.film_dic


if __name__ == "__main__":
    try:
        lbox_scrape = scraper()
        lbox_scrape.accept_cookies()
        link_list = lbox_scrape.get_links_from_several_pages(pages = 1)
        # link_list = ['https://letterboxd.com/film/the-matrix/', 'https://letterboxd.com/film/knives-out-2019/']
        film_data = lbox_scrape.scrape_info(link_list)
        lbox_scrape.driver.quit()
        print(pd.DataFrame.from_dict(film_data))
        # print(link_list)
    except:
        lbox_scrape.driver.quit()














