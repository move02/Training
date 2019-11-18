from bs4 import BeautifulSoup
from urllib import parse
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re
from fake_useragent import UserAgent

logfile = open("C:\pyworkspace\logs\\daum.log", "a")

# DataBase setting
db = pymysql.connect(
    host='keywordanalysis.cgtu9srx8vwg.ap-northeast-2.rds.amazonaws.com', 
    port=3306, 
    user='move02', 
    passwd='daummove02', 
    db='training5',
    charset='utf8', 
    cursorclass=pymysql.cursors.DictCursor
)

cursor = db.cursor()

news_insert_query = """INSERT INTO tb_news (
        press_id, 
        nid,
        published_date,
        created_date,
        title,
        body,
        url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

# about time
KST = timezone('Asia/Seoul')
now = datetime.now()
days_ago = datetime.now() - timedelta(days=3)

get_categories_query = """
    SELECT * FROM tb_category c
    JOIN press_category pc
    ON c.id = pc.category_id
    WHERE pc.press_id = 2
"""
ua = UserAgent()

emoji_pattern = re.compile(u"\U0001D800-\U0001F9FF", flags=re.UNICODE)
# emoji_pattern = re.compile("["
#         u"\U0001F600-\U0001F64F"  # emoticons
#         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#         u"\U0001F680-\U0001F6FF"  # transport & map symbols
#         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            "]+", flags=re.UNICODE)

cursor.execute(get_categories_query)
categories = cursor.fetchall()

base_url = "https://news.v.daum.net/v/"
main_url = "https://news.daum.net/breakingnews/"

count = 0

base_date = datetime.now()

def get_idlist_of_articles(category, last_date):
    url_list =[]
    reg_date = base_date
    i = 1
    while True:
        page = i
        reg_date_str = datetime.strftime(reg_date, "%Y%m%d")
        par = {"page" : page, "regDate" : reg_date_str}
        
        headers = {"user-agent" : ua.random}
        resp = requests.get(main_url + category['url'], params=par, headers=headers)
        # print(resp.request.url)
        soup = BeautifulSoup(resp.content, 'html.parser')
        print(par)
        # find list and iterate
        lis = soup.select("ul.list_allnews li")
        if len(lis) == 0:
            # 이전날짜로 가기
            reg_date = reg_date - timedelta(days=1)
            i = 1
            continue
            
        for li in lis:
            # press name
            press_name = li.select("span.info_news")[0].text
            if "뉴시스" in press_name or "뉴스1" in press_name or "연합뉴스" in press_name:
                continue
            # published_at 
            article_time = li.select("span.info_news > span.info_time")[0].text
            published_at_str = reg_date_str + article_time
            published_at = datetime.strptime(published_at_str, "%Y%m%d%H:%M")

            if published_at <= last_date:
                return url_list
            else:
                article_url = li.select("a.link_txt")[0]['href']
                article_id = article_url.split("/")[-1]
                article_title = li.select("a.link_txt")[0].text
                if "포토" in article_title:
                    continue
                # print("pr : {} / pubdate : {} / title : {}".format(press_name, published_at, article_title))
                url_list.append(article_url)
        i += 1

    return url_list

def parse_articles(url_list, last_date):
    articles_data = []
    global emoji_pattern
    for ar_url in url_list:
        article_url = ar_url
        article_id = article_url.split("/")[-1]

        print(article_url)

        headers = [
            {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"},
            {"user-agent" : 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)'},
            {"user-agent" : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36'},
            {"user-agent" : 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:62.0) Gecko/20100101 Firefox/62.0'},
        ]
        try:
            # headers = headers = {"user-agent" : ua.random}
            temp = 0
            resp = requests.get(article_url, headers=(headers[temp % len(headers)])).content
            temp += 1
            article_soup = BeautifulSoup(resp, 'html.parser')

            # title
            article_title = article_soup.select("h3.tit_view")[0].text.strip()
            # published_date
            try:
                infos = article_soup.select("span.txt_info")
                informations = ""
                for info in infos:
                    informations += info.text
                # format : 2019.10.08. 15:46
                match = re.search(r'\d{4}.\d{2}.\d{2}. \d{2}:\d{2}', informations)
                published_at = datetime.strptime(match.group(), "%Y.%m.%d. %H:%M")
            except Exception as ex:
                print("err ::::: {}".format(article_url))
                logfile.write(str(resp))
                raise(ex)

            if published_at <= last_date:
                return articles_data

            # body
            article_paragraphs = article_soup.select("div.article_view p")
            
            article_body = ""
            
            for paragraph in article_paragraphs:
                article_body += paragraph.text.strip().encode("UTF-8", "ignore").decode("UTF-8")

            article_body = emoji_pattern.sub(r'', article_body)
            article_for_insert = (
                2,
                article_id,
                datetime.strftime(published_at, '%Y-%m-%d %H:%M:%S'),
                datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
                article_title,
                article_body,
                article_url
            )

            print("id : {} / pubdate : {} / title : {}".format(article_id, published_at, article_title))
            articles_data.append(article_for_insert)
        except requests.exceptions.ConnectionError as conerr:
            print("conn errr")
            continue

    return articles_data

def retrieve_category(categories):
    global days_ago
    articles_data = []
    for category in categories:
        query = """
            SELECT * FROM tb_news n 
            JOIN news_category nc
            ON n.id = nc.news_id
            WHERE n.press_id = 2 and nc.category_id = %s ORDER BY published_date DESC LIMIT 1
        """

        cursor.execute(query, category['id'])
        last_article = cursor.fetchone()

        if last_article is None:
            # get_idlist_of_articles(category, days_ago)
            articles_data = parse_articles(get_idlist_of_articles(category, days_ago), days_ago)
        else:
            # get_idlist_of_articles(category, last_article['published_date'])
            articles_data = parse_articles(get_idlist_of_articles(category, last_article['published_date']), last_article['published_date'])
        
        insert_article_datas(articles_data, category['id'])

def insert_article_datas(article_datas, category_id):
    global count
    for art in article_datas:
        try:
            cursor.execute("SELECT * FROM tb_news WHERE nid like %s", art[1])
            result = cursor.fetchone()
            if result is None:
                cursor.execute(news_insert_query, art)
                cursor.execute("INSERT INTO news_category (news_id, category_id) VALUES (%s, %s)", (cursor.lastrowid, category_id))
                count += 1
            else:
                logfile.write("[{}] 중복 데이터 발견 nid : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[-1]))
                continue

        except Exception as ex:
            logfile.write("[{}] 데이터 삽입 중 에러 nid : {} errcode : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[2], ex))
            raise(ex)
            exit(1)

    db.commit()

retrieve_category(categories)
db.commit()
db.close()