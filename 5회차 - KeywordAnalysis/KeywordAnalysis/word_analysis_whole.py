from datetime import datetime, timedelta
from pytz import timezone, utc
import pymysql
import re
from khaiii import KhaiiiApi
from khaiii.khaiii import KhaiiiExcept
from multiprocessing import Process, Queue

api = KhaiiiApi()
logfile = open("./logs/analysis.log", "a")

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

KST = timezone('Asia/Seoul')
now = datetime.now()
today = datetime.strftime(datetime(now.year, now.month, now.day), '%Y-%m-%d %H:%M:%S')
yesterday = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d %H:%M:%S')

get_categories_query = """
    SELECT * FROM press_category
    WHERE press_id = %s
"""

get_articles_query = """
    SELECT * FROM tb_news n
    WHERE (n.id BETWEEN %s AND %s)
    AND n.published_date <= %s
"""

get_presses_query = """
    SELECT * FROM tb_press
"""

insert_word_query = """
    INSERT INTO tb_word
    (term, pos, frequency, news_id, extracted_date)
    VALUES (%s, %s, %s, %s, %s)
"""

# cursor.execute(get_presses_query)
# presses = cursor.fetchall()

# for press in presses:    
#     cursor.execute(get_categories_query, press['id'])
#     categories = cursor.fetchall()

#     for category in categories:
#         cursor.execute(get_articles_query, (press['id'], category['category_id']))
#         articles = cursor.fetchall()

replacements = {
    "문 대통령" : "문재인 대통령",
    "조 장관" : "조국 장관",
    "김 위원장" : "김정은 위원장",
    "윤 총장" : "윤석열 총장"
}

def replace_names(doc):
    for key in replacements.keys():
        doc.replace(key, replacements[key])
    return doc

def tag_and_insert(start, end, result):    
    db_new = pymysql.connect(
        host='keywordanalysis.cgtu9srx8vwg.ap-northeast-2.rds.amazonaws.com', 
        port=3306, 
        user='move02', 
        passwd='daummove02', 
        db='training5',
        charset='utf8', 
        cursorclass=pymysql.cursors.DictCursor
    )

    cursor_new = db_new.cursor()
    cursor_new.execute(get_articles_query, (start, end, yesterday))
    articles = cursor_new.fetchall()
    count = 1

    for article in articles: 
        # print("{} . title : {}".format(count, article['title']))
        words = None
        try:
            if len(article['body']) == 0:
                continue
            
            words = api.analyze(replace_names(article['body']))
        except khaiii.KhaiiiExcept as khaiiierr:
            print("khaiii err")
            print(article['body'])
            continue

        lex_freq_dict = {}
        lex_tag_dict = {}

        for word in words:
            for morph in word.morphs:
                if ("NNP" == morph.tag) and len(morph.lex) > 1:
                    if lex_freq_dict.get(morph.lex) is None:
                        lex_freq_dict[morph.lex] = 1 
                        lex_tag_dict[morph.lex] = morph.tag 
                    else:
                        lex_freq_dict[morph.lex] += 1

        for lex, freq in lex_freq_dict.items():
            # (term, pos, frequency, news_id, extracted_date)
            cursor_new.execute(insert_word_query, (lex, lex_tag_dict[lex], freq, article['id'], article['published_date']))
        count += 1
        if count >= 200:
            db_new.commit()
            count = 1
    db_new.commit()
    db_new.close()
    return

cursor.execute("SELECT count(id) from tb_news where published_date <= %s", today)
amount = int(cursor.fetchone()['count(id)'])

print("amount : {}".format(amount))

proc1 = Process(target=tag_and_insert, args=(1, (amount // 2) - 1))
proc2 = Process(target=tag_and_insert, args=((amount // 2), amount)
proc1.start()
proc2.start()
proc1.join()
proc2.join()

db.close()


# res = sorted(lex_freq_dict.items(), key=(lambda x: x[1]), reverse = False)
# print(res)
# exit(1)