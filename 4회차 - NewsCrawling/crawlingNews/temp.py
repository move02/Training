from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re

url = "https://www.huffingtonpost.kr/politics/"
headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"}
resp = requests.get(url, headers=headers)

body = resp.content
print(body)
# soup = BeautifulSoup(body, 'html.parser')

# article_set = soup.select("div.section-list-article")