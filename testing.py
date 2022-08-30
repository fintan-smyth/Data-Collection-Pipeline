from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import time


class scraper:

    def __init__(self):

        self.driver = webdriver.Firefox()
        # self.driver.set_page_load_timeout(10)
        self.URL = "https://letterboxd.com/films/popular/"
        self.film_dic = {'Title': [], 'Year': [], 'Director': [], 'Runtime': [], 'Rating': [], 'Watches': [], 'Lists': [], 'Likes': [], 'Top 250 Position': [], 'Description': []}
        self.driver.get(self.URL)


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

    def close_ads(self):
        
        ad_container = self.driver.find_element(by=By.XPATH, value='//*[@id="pw-oop-bottom_rail"]')
        # driver.switch_to.element('pw-oop-bottom_rail')
        close_button = ad_container.find_element(by=By.XPATH, value='//*[starts-with(@style,"background")]')
        close_button.click()


    def get_links(self):
        delay = 10
        WebDriverWait(self.driver, delay).until(EC.presence_of_element_located((By.XPATH, '//*[@class="poster-list -p70 -grid"]')))
        print('Poster list ready...')
        film_container = self.driver.find_element(by=By.XPATH, value='//*[@class="poster-list -p70 -grid"]')
        film_list = film_container.find_elements(by=By.XPATH, value='./li')
        link_list = []

        for film in film_list:
            a_tag = film.find_element(By.TAG_NAME, 'a')
            link = a_tag.get_attribute('href')
            link_list.append(link)
        print('Links scraped.\n')
        return link_list

    def get_links_from_several_pages(self, pages):
        
        big_list = []
        big_list.extend(self.get_links())
        
        next_page = 2
        for i in range(pages-1):
            time.sleep(1)
            next_page_url = f'https://letterboxd.com/films/popular/size/small/page/{next_page}/'
            self.driver.get(next_page_url)
            next_page += 1
            big_list.extend(self.get_links())
        
        return big_list
    
    def scrape_info(self, links):
        
        delay = 5
        try:
            for link in links:
                driver = self.driver
                driver.get(link)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//h1[@class="headline-1 js-widont prettify"]')))
                title = driver.find_element(by=By.XPATH, value='//h1[@class="headline-1 js-widont prettify"]').text
                print(f'Title: {title}')
                self.film_dic['Title'].append(title)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@href,"/films/year/")]')))
                year = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/films/year/")]').text
                print(f'Year: {year}')
                self.film_dic['Year'].append(year)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@href,"/director/")]')))
                director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]').text
                directors = [director]
                try:
                    next_director = driver.find_element(by=By.XPATH, value='//a[starts-with(@href,"/director/")]/following-sibling::a').text
                    directors.append(next_director)
                except:
                    pass
                if len(directors) > 1:
                    self.film_dic['Director'].append(directors)
                    print(f'Directors: {directors}')
                else:
                    self.film_dic['Director'].append(director)
                    print(f'Director: {director}')
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//p[@class="text-link text-footer"]')))
                runtime = driver.find_element(by=By.XPATH, value='//p[@class="text-link text-footer"]').text.partition('  ')[0]
                print(f'Runtime: {runtime}')
                self.film_dic['Runtime'].append(runtime)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[starts-with(@class,"tooltip display-rating")]')))
                avg_rating = driver.find_element(by=By.XPATH, value='//a[starts-with(@class,"tooltip display-rating")]').text
                print(f'Rating: {avg_rating}')
                self.film_dic['Rating'].append(avg_rating)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[@class="has-icon icon-watched icon-16 tooltip"]')))
                n_watches = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-watched icon-16 tooltip"]').text
                print(f'Watches: {n_watches}')
                self.film_dic['Watches'].append(n_watches)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[@class="has-icon icon-list icon-16 tooltip"]')))
                n_lists = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-list icon-16 tooltip"]').text
                print(f'Lists: {n_lists}')
                self.film_dic['Lists'].append(n_lists)
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]')))
                n_likes = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-like icon-liked icon-16 tooltip"]').text
                print(f'Likes: {n_likes}')
                self.film_dic['Likes'].append(n_likes)
                try:
                    top_250_pos = driver.find_element(by=By.XPATH, value='//a[@class="has-icon icon-top250 icon-16 tooltip"]').text
                    print(f'Top 250 position: {top_250_pos}')
                    self.film_dic['Top 250 Position'].append(top_250_pos)
                except:
                    self.film_dic['Top 250 Position'].append('N/A')
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.XPATH, '//div[@class="review body-text -prose -hero prettify"]')))
                try:
                    more_button = driver.find_element(by=By.XPATH, value='//span[@class="condense_control condense_control_more"]')
                    more_button.click()
                    description = driver.find_element(by=By.XPATH, value='//div[@class="truncate condenseable"]').text.split('×')[0]   
                except:
                    description = driver.find_element(by = By.XPATH, value='//div[@class="review body-text -prose -hero prettify"]//p').text
                print(f'Description: {description}\n')
                self.film_dic['Description'].append(description)
                time.sleep(1)
        except:
            print('\nAborted')
        return self.film_dic


if __name__ == "__main__":
    lbox_scrape = scraper()
    lbox_scrape.accept_cookies()
    link_list = lbox_scrape.get_links_from_several_pages(pages = 1)
    # link_list = ['https://letterboxd.com/film/ratatouille/', 'https://letterboxd.com/film/lady-bird/', 'https://letterboxd.com/film/dune-2021/', 'https://letterboxd.com/film/the-grand-budapest-hotel/', 'https://letterboxd.com/film/once-upon-a-time-in-hollywood/', 'https://letterboxd.com/film/la-la-land/', 'https://letterboxd.com/film/whiplash-2014/', 'https://letterboxd.com/film/avengers-infinity-war/', 'https://letterboxd.com/film/the-wolf-of-wall-street/', 'https://letterboxd.com/film/everything-everywhere-all-at-once/', 'https://letterboxd.com/film/the-shining/']
    film_data = lbox_scrape.scrape_info(link_list)
    lbox_scrape.driver.quit()
    # print(link_list)














