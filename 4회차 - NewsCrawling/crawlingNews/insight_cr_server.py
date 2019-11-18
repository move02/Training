from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re

logfile = open("logs/insight.log", "w")
count = 0

# DataBase setting
db = pymysql.connect(
    host='dscrawler2.cgtu9srx8vwg.ap-northeast-2.rds.amazonaws.com', 
    port=3306, 
    user='move02', 
    passwd='daummove02', 
    db='training4',
    charset='utf8', 
    cursorclass=pymysql.cursors.DictCursor
)

cursor = db.cursor()

news_insert_query = """INSERT INTO tb_news (
        press_id, 
        journalist_id,
        nid,
        published_date,
        created_date,
        title,
        body,
        url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

date_splitter = " · "

# about time
KST = timezone('Asia/Seoul')
now = datetime.now()
days_ago = datetime.now() - timedelta(days=7)

# Logic 시작
category_dict = {"movie" : 1, "sports" : 2}

url = "https://www.insight.co.kr/section/"

def get_set_of_single_category(category, url):
    query = """
        SELECT * FROM tb_news n 
        JOIN news_category nc
        ON n.id = nc.news_id
        WHERE n.press_id = 3 and nc.category_id = %s ORDER BY published_date DESC LIMIT 1
    """

    cursor.execute(query, category_dict[category])
    last_article = cursor.fetchone()

    article_id_set = set([])
    params={'page' : 0}
    for i in range(1,20):
        params['page'] = i

        resp = requests.get(url + category, params=params)

        body = resp.content
        soup = BeautifulSoup(body, 'html.parser')

        article_set = soup.select("div.section-list-article")

        for article in article_set:
            headline = article.find("a", class_="section-list-article-title")
            article_url = headline['href']
            byline = article.find("span", class_="section-list-article-byline").text
            
            published_at = datetime.strptime(byline.split(date_splitter)[1], '%Y-%m-%d %H:%M:%S')
            
            if last_article is not None:
                if last_article['published_date'] >= published_at:
                    return article_id_set

            if days_ago >= published_at:
                return article_id_set
            else:
                article_id = article_url.split("/")[-1]
                article_id_set.add(article_id)


def get_set_of_articles(category, url):
    article_id_set = set([])
    article_id_set = article_id_set.union(get_set_of_single_category(category, url))
    
    return article_id_set

def get_journalist(journalist_field):
    writer_name = journalist_field.find("span", class_="news-byline-writer").text.replace(" 기자", "")
    writer_email = journalist_field.find("span", class_="news-byline-mail")

    result = None
    if writer_email is None:
        cursor.execute("SELECT * FROM tb_journalist WHERE press_id = 3 and name like %s LIMIT 1", writer_name)
        result = cursor.fetchone()
        if result is None:
            cursor.execute("INSERT INTO tb_journalist (press_id, name) VALUES (3, %s)", (writer_name))
            db.commit()
            cursor.execute("SELECT * FROM tb_journalist WHERE press_id = 3 and name like %s", writer_name)
            result = cursor.fetchone()
    else:
        writer_email = writer_email.text
        cursor.execute("SELECT * FROM tb_journalist WHERE email like %s", writer_email)
        result = cursor.fetchone()
        if result is None:
            cursor.execute("INSERT INTO tb_journalist (press_id, name, email) VALUES (3, %s, %s)", (writer_name, writer_email))
            db.commit()
            cursor.execute("SELECT * FROM tb_journalist WHERE email like %s", writer_email)
            result = cursor.fetchone()

    return result['id']

def parse_articles(article_set):
    base = "https://www.insight.co.kr/news/"

    article_datas = []

    for article_id in article_set: 
        url = base + article_id

        resp = requests.get(url)

        body = resp.content
        soup = BeautifulSoup(body, 'html.parser')

        try:
            # header
            article_header = soup.select("div.news-header > h1")[0].text
            
            # 기자 정보, 날짜
            byline = soup.select("div.news-container > div.news-byline")[0]
            
            journalist_id = int(get_journalist(byline))

            article_date = byline.find("em", class_="news-byline-date-send").text
            match = re.search(r'\d{4}.\d{2}.\d{2} \d{2}:\d{2}', article_date)
            article_date = datetime.strftime(datetime.strptime(match.group(), '%Y.%m.%d %H:%M'),'%Y-%m-%d %H:%M:%S')

            # article 내용
            article_body = soup.select("div.news-article > div.news-article-memo")[0]

            for ads in article_body.select("ins.adsbygoogle"):
                ads.decompose()

            for imgs in article_body.select("img"):
                imgs.parent.decompose()

            article_text = ""
            for texts in article_body.select("p"):
                article_text += texts.text

            article_text.replace(u"\xa0", " ")

            article_datas.append((
                    3,
                    journalist_id,
                    article_id,
                    article_date,
                    datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
                    article_header,
                    article_text,
                    url
                )
            )

        except Exception as ex :
            logfile.write("[{}] 데이터 파싱 중 에러 url : {} errcode : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), url, ex))
            raise(ex)

    return article_datas
                    
def insert_article_datas(article_datas, category_id):
    for art in article_datas:
        try:
            cursor.execute("SELECT FROM tb_news WHERE nid LIKE %s", art[2])
            result = cursor.fetchone()
            if result is None:
                cursor.execute(news_insert_query, art)
                cursor.execute("INSERT INTO news_category (news_id, category_id) VALUES (%s, %s)", (cursor.lastrowid, category_id))
                count += 1
            else:
                logfile.write("[{}] 중복 데이터 삽입 nid : {}]n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[2]))
        except Exception as ex:
            logfile.write("[{}] 데이터 삽입 중 에러 nid : {} errcode : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[2], ex))
            raise(ex)
            exit(1)
    # cursor.executemany(news_insert_query, article_datas)


for key in category_dict.keys():
    article_datas = parse_articles(get_set_of_articles(key, url))
    insert_article_datas(article_datas, category_dict[key])

db.commit()
db.close()

logfile.write("[{}]insight 크롤링 완료. {} 개 신규 기사".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), count))