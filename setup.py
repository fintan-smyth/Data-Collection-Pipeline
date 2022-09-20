from setuptools import setup
from setuptools import find_packages

setup(
    name='letterboxd_scraper_test',
    version='0.0.1',
    description='Tool to scrape letterboxd.com for film data',
    url='',
    author='Fintan Smyth',
    packages=find_packages(),
    install_requires=['requests', 'selenium'],
)