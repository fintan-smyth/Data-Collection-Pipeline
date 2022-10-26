FROM python:3.10-slim

RUN apt-get update && apt -y upgrade &&  apt-get install -y firefox-esr 
RUN apt install wget -y && apt-get -y install libpq-dev gcc

RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux64.tar.gz \
    && tar -xvzf geckodriver* \
    && chmod +x geckodriver \
    && mv geckodriver /usr/local/bin

COPY data_collection/scraper.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "scraper.py"]