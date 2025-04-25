from bs4 import BeautifulSoup
import requests

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)
years = ['2019', '2020', '2021', '2022']


def get_list_yellow_url():
    yellow_taxi = []
    if response.status_code == 200:
        doc = BeautifulSoup(response.content, 'html.parser')
        list_link = doc.find_all('a', href=True)
        for link in list_link:
            href = link['href']
            if 'yellow' in href and any(year in href for year in years):
                yellow_taxi.append(href)
    return yellow_taxi

def get_list_green_url():
    years = ['2019', '2020', '2021', '2022']
    green_taxi = []
    if response.status_code == 200:
        doc = BeautifulSoup(response.content, 'html.parser')
        list_link = doc.find_all('a', href=True)
        for link in list_link:
            href = link['href']
            if 'green' in href and any(year in href for year in years):
                green_taxi.append(href)
    return green_taxi

def get_list_fhv_url():
    years = ['2019', '2020', '2021', '2022']
    fhv = []
    if response.status_code == 200:
        doc = BeautifulSoup(response.content, 'html.parser')
        list_link = doc.find_all('a', href=True)
        for link in list_link:
            href = link['href']
            if 'fhv' in href and any(year in href for year in years):
                fhv.append(href)
    return fhv
