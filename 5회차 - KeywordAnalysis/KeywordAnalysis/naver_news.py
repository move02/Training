from bs4 import BeautifulSoup
from urllib import parse
import requests
from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re
from selenium import webdriver

logfile = open("C:\pyworkspace\logs\\naver.log", "a")

# selenium settings
options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument('--blink-settings=imagesEnabled=false')
options.add_argument("disable-gpu")
options.add_argument('log-level=3')
options.add_argument('--disable-logging')

driver = webdriver.Chrome('C:\pyworkspace\chromedriver.exe', chrome_options=options)

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
days_ago = datetime.now() - timedelta(days=7)

get_categories_query = """
    SELECT * FROM tb_category c
    JOIN press_category pc
    ON c.id = pc.category_id
    WHERE pc.press_id = 1
"""

emoji_pattern = re.compile(u"\U0001D800-\U0001F9FF", flags=re.UNICODE)
# emoji_pattern = re.compile("["
#         u"\U0001F600-\U0001F64F"  # emoticons
#         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#         u"\U0001F680-\U0001F6FF"  # transport & map symbols
#         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            "]+", flags=re.UNICODE)

cursor.execute(get_categories_query)
categories = cursor.fetchall()

base_url = "https://news.naver.com"
main_url = "https://news.naver.com/main/main.nhn?"

count = 0

def get_idlist_of_articles(category, last_date):
    url_list =[]
    for i in range(1, 100):
        page = i
        # #&date=%2000:00:00&page=100
        par = {"sid1" : category['url']}

        qstr = parse.urlencode(par)

        driver.get(main_url + qstr + "#&date=%2000:00:00&page=" + str(page))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # #section_body > ul.type06_headline > li:nth-child(1) > dl > dt:nth-child(2) > a
        links = soup.select("#section_body > ul > li")

        for link in links:
            outdate = link.select("dl > dd > span.date")[0].text
            if "일전" in outdate:
                if datetime.now() - timedelta(days=int(outdate[0])) >= last_date:
                    print("before last date")
                    return url_list
            a_tag = link.select("dl > dt:nth-child(1) > a")[0]
            url_list.append(base_url+a_tag['href'])
            
        if soup.select("a._paging.next") is None:
            print("next page btn is none")
            return url_list

    return url_list

def parse_articles(url_list, last_date):
    articles_data = []
    global emoji_pattern
    headers = {"user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36"}
    for ar_url in url_list:
        article_url = parse.urlparse(ar_url)
        parameters = parse.parse_qs(article_url.query)
        article_id = '&oid=' + parameters['oid'][0] + '&aid= '+ parameters['aid'][0]

        print(article_url.geturl())

        resp = requests.get(article_url.geturl(), headers=headers).content
        article_soup = BeautifulSoup(resp, 'html.parser')

        # driver.save_screenshot("./screenshot.png")

        # title
        article_title = article_soup.select("h3#articleTitle")[0].text.strip().encode("UTF-8", "ignore").decode("UTF-8")
        article_title = emoji_pattern.sub(r'', article_title)
        # published_date
        published_at = article_soup.select("#main_content > div.article_header > div.article_info > div > span.t11")[0].text
        published_at = published_at.replace("오후", "PM").replace("오전", "AM")
        # format : 2019.10.08. 오전 11:08
        published_at = datetime.strptime(published_at, "%Y.%m.%d. %p %I:%M")

        if published_at <= last_date:
            return articles_data

        # body
        article_body = article_soup.find("div", id="articleBodyContents")

        for ads in article_body.select("script"):
            ads.decompose()

        for paragraph in article_body.select("p"):
            paragraph.decompose()
            
        for li in article_body.select("li"):
            li.decompose()

        for img in article_body.select("span.end_photo_org"):
            img.decompose()
        
        article_body = article_body.text.strip().encode("UTF-8", "ignore").decode("UTF-8")
        article_body = emoji_pattern.sub(r'', article_body)
        article_for_insert = (
            1,
            article_id,
            datetime.strftime(published_at, '%Y-%m-%d %H:%M:%S'),
            datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'),
            article_title,
            article_body,
            article_url.geturl()
        )

        print("id : {} / pubdate : {} / title : {}".format(article_id, published_at, article_title))
        articles_data.append(article_for_insert)

    return articles_data

def retrieve_category(categories):
    global days_ago
    articles_data = []
    for category in categories:
        query = """
            SELECT * FROM tb_news n 
            JOIN news_category nc
            ON n.id = nc.news_id
            WHERE n.press_id = 1 and nc.category_id = %s ORDER BY published_date DESC LIMIT 1
        """

        cursor.execute(query, category['id'])
        last_article = cursor.fetchone()

        print("last_article : {}".format(last_article))

        if last_article is None:
            articles_data = parse_articles(get_idlist_of_articles(category, days_ago), days_ago)
        else:
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
                check_category_query = """
                    SELECT * FROM tb_news n
                    JOIN news_category nc
                    ON n.id = nc.news_id
                    WHERE n.id = %s
                """
                cursor.execute(check_category_query, result['id'])
                temp = cursor.fetchone()
                if int(temp['category_id']) != category_id:
                    cursor.execute("INSERT INTO news_category (news_id, category_id) VALUES (%s, %s)", (result['id'], category_id))
                else:
                    logfile.write("[{}] 중복 데이터 발견 nid : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[-1]))

        except Exception as ex:
            logfile.write("[{}] 데이터 삽입 중 에러 nid : {} errcode : {}\n".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), art[2], ex))
            raise(ex)
            exit(1)

retrieve_category(categories)
driver.close()
db.commit()
db.close()