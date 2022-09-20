# Data-Collection-Pipeline

The task for this project is to create a data collection pipeline that will scrape data from a chosen website and scalably store it online. Ultimately a full data pipeline will be created including running the scraper on a cloud server and monitoring.

# Milestone 1

The task for the first milestone is to choose the website that I will scrape data from. The website should have a many items from which a good amount of similar datapoints can be obtained, ideally including images. Based on these criteria I chose the film comparison website Letterboxd.com as it has an extensive database of films, from which many different datapoints can be extracted such as title, release year, director as well as an image of the film's poster. 

# Milestone 2

The task for the second milestone is to start building my `scraper` class by creating methods to perform basic navigation of the website and obtain links to entries from which data can be scraped.

I started by creating a new file `scraper.py` and importing all the necessary modules I will need to run my scraper. The `scraper` class is then created and the `__init__` method is defined:
```python
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
        self.driver.get(self.start_url)
```
- The primary package I am importing from is `selenium`. Selenium webdriver is a tool that allows you to automatically drive a web browser and use it to interact with webpages. For this project I can use it to navigate my chosen website and extract data from the site's `html`.

- I then define the `scraper` class. It accepts one parameter `start_page` with a default value of 1. This dictates the first page of the `letterboxd.com/films/popular` category that the scraper will use to obtain links to film entries.
- A Firefox webdriver instance is called and assigned to the attribute `driver`.
- The `start_page` parameter is assigned to the `start_page` attribute.
- The string `'https://letterboxd.com/films/popular/page/{self.start_page}'` is formatted with the `start_page` attribute and assigned to the attribute `start_url`.
- The webdriver then loads the webpage assigned to `start_url`

---

The next method I defined is the `accept_cookies` method. Upon loading the website on a fresh browser a cookies notification pops up that prevents some website interaction, and so a method to close this should be created to allow the scraper to function.
```python
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
```
- The driver waits for the element containing the accept cookies button to appear.
    - When the driver detects this element `Accept cookies button ready...` is printed in the console.
    - The accept cookies button element is assigned to `accept_cookies_button` and the driver is instructed to click it. 
    - `Cookies accepted.` is printed in the console.
- 