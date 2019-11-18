from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re
from selenium import webdriver

logfile = open("/home/ubuntu/crawlers/logs/huff.log", "w")
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

headers = {
    "accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "accept-encoding" : "gzip, deflate, br",
    "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"
}

# selenium settings
# options = webdriver.ChromeOptions()
# options.add_argument('headless')
# options.add_argument('window-size=1920x1080')
# options.add_argument("disable-gpu")
# options.add_argument('log-level=3')
# options.add_argument('--disable-logging')

# driver = webdriver.Chrome('C:\pyworkspace\chromedriver.exe', chrome_options=options)
# article_driver = webdriver.Chrome('C:\pyworkspace\chromedriver.exe', chrome_options=options)

# about time
KST = timezone('Asia/Seoul')
now = datetime.now()
days_ago = datetime.now() - timedelta(days=20)

# Logic 시작
category_dict = {"movie" : 1, "sports" : 2}

url = "https://www.huffingtonpost.kr/news/"
entry_url = "https://www.huffingtonpost.kr"

def get_set_of_single_category(category, url):
    query = """
        SELECT * FROM tb_news n 
        JOIN news_category nc
        ON n.id = nc.news_id
        WHERE n.press_id = 4 and nc.category_id = %s ORDER BY published_date DESC LIMIT 1
    """

    cursor.execute(query, category_dict[category])
    last_article = cursor.fetchone()
    
    article_id_set = set([])
    end_flag = False
    for i in range(1,20):
        if end_flag is True:
            return article_id_set
        temp_url = url + category + "/" + str(i)

        body = requests.get(temp_url, headers=headers).content
        soup = BeautifulSoup(body, 'html.parser')


        article_set = soup.select("div.apage-rail-cards div.card__content")

        for article in article_set:
            article_url = article.find("a", class_="card__link")['href']
            article_body = requests.get(entry_url + article_url, headers=headers).content
            article_soup = BeautifulSoup(article_body, 'html.parser')

            published_at = article_soup.find("span", class_="timestamp__date--published").text.replace("\n", "").strip()
            published_at = datetime.strptime(published_at[:-4], '%Y년 %m월 %d일 %H시 %M분')
            
            if last_article is not None:
                if last_article['published_date'] >= published_at:
                    end_flag = True
                    continue

            if days_ago >= published_at:
                return article_id_set
            else:
                article_id = (entry_url + article_url).split("/")[-1].split("?")[0]
                article_id_set.add(article_id)


def get_set_of_articles(category, url):
    article_id_set = set([])
    article_id_set = article_id_set.union(get_set_of_single_category(category, url))
    return article_id_set

def get_journalist(journalist_field):
    try:
        writer_id = journalist_field.find("a", class_="author-card__details__name")
        if writer_id is not None:
            writer_id = writer_id['href'].split("/")[-1]
        writer_name = journalist_field.find("span", class_="author-card__details__name")
        if writer_name is not None:
            writer_name = writer_name.text.strip()
        else:
            writer_name = "anonymous"
        microbio = journalist_field.find("span", class_="author-card__microbio")
        if writer_name is not None:
            microbio = microbio.text
        else:
            microbio = "anonymous@huffpost.kr"

    except KeyError:
        writer_id = None
        writer_name = journalist_field.select(".author-card__details__name")[0].text.strip()
        microbio = journalist_field.find("span", class_="author-card__microbio").text
    
    # email regex
    writer_email = None
    match = re.search(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", microbio)
    if(match is not None):
        writer_email = match.group()

    result = None
    if writer_id is None:
        if writer_email is None:
            cursor.execute("SELECT * FROM tb_journalist WHERE name like %s LIMIT 1", writer_name)
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO tb_journalist (press_id, name) VALUES (4, %s)", (writer_name))
                db.commit()
                cursor.execute("SELECT * FROM tb_journalist WHERE name like %s LIMIT 1", writer_name)
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT * FROM tb_journalist WHERE email like %s LIMIT 1", writer_email)
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO tb_journalist (press_id, name, email) VALUES (4, %s, %s)", (writer_name, writer_email))
                db.commit()
                cursor.execute("SELECT * FROM tb_journalist WHERE email like %s", writer_email)
                result = cursor.fetchone()

    else:
        if writer_email is None:
            cursor.execute("SELECT * FROM tb_journalist WHERE jid like %s LIMIT 1", writer_id)
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO tb_journalist (press_id, name, jid) VALUES (4, %s, %s)", (writer_name, writer_id))
                db.commit()
                cursor.execute("SELECT * FROM tb_journalist WHERE jid like %s LIMIT 1", writer_id)
                result = cursor.fetchone()
        else:
            cursor.execute("SELECT * FROM tb_journalist WHERE jid like %s LIMIT 1", writer_id)
            result = cursor.fetchone()
            if result is None:
                cursor.execute("INSERT INTO tb_journalist (press_id, name, jid, email) VALUES (4, %s, %s, %s)", (writer_name, writer_id, writer_email))
                db.commit()
                cursor.execute("SELECT * FROM tb_journalist WHERE jid like %s", writer_id)
                result = cursor.fetchone()
    
    return result['id']

def parse_articles(article_set):
    article_datas = []

    for article_id in article_set: 
        url = entry_url + "/entry/" + article_id
        body = requests.get(url, headers=headers).content
        soup = BeautifulSoup(body, 'html.parser')

        try:
            # header
            article_header = soup.select("div.headline > h1")[0].text
            
            # 기자 정보, 날짜
            #entry-footer > div.author-byline.author-byline--footer li.author-card
            journalist_field = soup.select("div.yr-entry-footer > div.author-byline.author-byline--footer > ul > li > div.author-card__details")[0]
            journalist_id = int(get_journalist(journalist_field))

            published_at = soup.select("span.timestamp__date.timestamp__date--published")[0].text.replace("\n", "").strip()
            article_date = datetime.strptime(published_at[:-4], '%Y년 %m월 %d일 %H시 %M분')
            article_date = datetime.strftime(article_date,'%Y-%m-%d %H:%M:%S')

            # article 내용
            article_body = soup.select("div.post-contents div.content-list-component.text")
            article_text = ""

            for paragraph in article_body:
                for ads in paragraph.select(".ad_spot"):
                    ads.decompose()

                for more_ads in paragraph.select("advertisement-holder"):
                    more_ads.decompose()

                article_text += paragraph.text

            article_datas.append((
                    4,
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
    global count
    for art in article_datas:
        try:
            cursor.execute("SELECT * FROM tb_news WHERE nid like %s", art[2])
            result = cursor.fetchone()
            if result is None:
                cursor.execute(news_insert_query, art)
                cursor.execute("INSERT INTO news_category (news_id, category_id) VALUES (%s, %s)", (cursor.lastrowid, category_id))
                count += 1
            else:
                logfile.write("[{}] 중복 데이터 삽입 nid : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[2]))
                
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

logfile.write("[{}]Huffington Post Kr 크롤링 완료. {} 개 신규 기사\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), count))