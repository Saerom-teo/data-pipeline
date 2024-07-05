# -*- coding: utf-8 -*-
# 필요한 모듈 Import
import urllib.request
import json
import pytz

# 사용할 Operator Import
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 디폴트 설정
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

NAVER_CLIENT_ID = ""
NAVER_CLIENT_SECRET = ""

NYT_CLIENT_SECRET = ""

BASE_NAVER_URL = "https://openapi.naver.com/v1/search/news.json"

BASE_NYT_URL = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

# start : 어느 인덱스부터 불러올 건지
# display : 몇 개 불러올건지
# sort : 날짜순, 정확도순 선택 가능 (date, slim)
# query : 검색어
def fetch_naver_news(start, display, sort, query):
    query_encoded =urllib.parse.quote(query)
    url = f"{BASE_NAVER_URL}?start={start}&display={display}&sort={sort}&query={query_encoded}"
    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", NAVER_CLIENT_ID)
    request.add_header("X-Naver-Client-Secret", NAVER_CLIENT_SECRET)

    response = urllib.request.urlopen(request)
    if(response.getcode() == 200):
        return json.loads(response.read().decode('utf-8'))
    else:
        return None

# 뉴욕 타임즈 뉴스 불러오기
def fetch_nytimes_articles(query, page=0):
    query_encoded = urllib.parse.quote(query)
    url = f"{BASE_NYT_URL}?q={query_encoded}&page={page}&api-key={NYT_CLIENT_SECRET}"
    request = urllib.request.Request(url)

    response = urllib.request.urlopen(request)
    if response.getcode() == 200:
        return json.loads(response.read().decode('utf-8'))
    else:
        return None

# 뉴욕 타임즈 뉴스 저장
def fetch_and_prepare_nytimes_result(**kwargs):

    sql_insert_data = ""

    nytimes_response = fetch_nytimes_articles('environment recycling')

    if not nytimes_response or nytimes_response['response']['meta']['hits'] == 0:
        return

    for item in nytimes_response['response']['docs']:
        title = item['headline']['main'].replace("'", "''") if 'headline' in item and 'main' in item['headline'] else 'No title'
        description = item.get('abstract', '').replace("'", "''") if item.get('abstract') else 'No description'
        description = description[:255]
        pub_date = datetime.strptime(item['pub_date'], '%Y-%m-%dT%H:%M:%S%z').astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S') if 'pub_date' in item else '1970-01-01 00:00:00'
        link = item['web_url'].replace("'", "''") if 'web_url' in item else 'No link'

        sql_insert_data += f"INSERT INTO NEWS(NEWSTITLE, NEWSDESCRIPTION, NEWSPUBDATE, NEWSURL, NEWSCATEGORY, NEWSCHECK) VALUES('{title}', '{description}', '{pub_date}', '{link}', '외국 뉴스', TRUE);\n"


    kwargs['ti'].xcom_push(key='insert_nyt_news_data', value=sql_insert_data)

def fetch_and_prepare_result(**kwargs):
    sql_insert_data = ""

    naver_response = fetch_naver_news(1, 100, 'date', '환경 친환경 분리수거')

    naver_total = naver_response['total']
    if(naver_total == 0 or naver_response == None):
        return

    for item in naver_response['items']:
        title = item['title'].replace("'", "''")
        description = item['description'].replace("'", "''")
        pubDate = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S %z').astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        link = item['link'].replace("'", "''")
        

        sql_insert_data += f"INSERT INTO NEWS(NEWSTITLE, NEWSDESCRIPTION, NEWSPUBDATE, NEWSURL, NEWSCATEGORY, NEWSCHECK) VALUES('{title}', '{description}', '{pubDate}', '{link}', '한국 뉴스', TRUE);\n"

    kwargs['ti'].xcom_push(key='insert_news_data', value=sql_insert_data)

sql_create_news_table = """
    CREATE TABLE IF NOT EXISTS `NEWS` (
        `NEWSID`	INT	PRIMARY KEY AUTO_INCREMENT,
        `NEWSCHECK`	BOOLEAN	NOT NULL DEFAULT FALSE,
        `NEWSTITLE` VARCHAR(255) NOT NULL,
        `NEWSDESCRIPTION` VARCHAR(255) NOT NULL,
        `NEWSPUBDATE` DATETIME NOT NULL,
        `NEWSURL` VARCHAR(255) NOT NULL,
        `NEWSCATEGORY` VARCHAR(255) NOT NULL
    );
"""

sql_create_news_full_index_1 = """
    CREATE FULLTEXT INDEX IF NOT EXISTS NEWS_FULLTEST_TITLE_INDEX ON NEWS(NEWSTITLE) WITH PARSER ngram;
"""

sql_create_news_full_index_2 = """
    CREATE FULLTEXT INDEX IF NOT EXISTS NEWS_FULLTEST_DESC_INDEX ON NEWS(NEWSDESCRIPTION) WITH PARSER ngram;
"""

sql_truncate_news_table = """
    TRUNCATE TABLE NEWS;
"""

sql_update_news_date = """
    INSERT INTO GLOBAL_SETTINGS(GLOBAL_KEY, GLOBAL_VALUE)
    VALUE ('news_update_date', DATE_FORMAT(SYSDATE(), '%Y-%m-%d %H:%i:%s'))
    ON DUPLICATE KEY UPDATE
    GLOBAL_VALUE=DATE_FORMAT(SYSDATE(), '%Y-%m-%d %H:%i:%s');
"""

with DAG(
    'news_crawling',
    default_args = default_args,
    description = 'CREATE NEWS TABLE AND TRANSFORM AND INSERT DATA IN MYSQL',
    schedule_interval = "@daily",
    start_date = datetime(2024,6,1),
    catchup = False,
    tags = ['naver', 'news', 'api', 'crawling', 'mysql'],
) as dag:
    # 뉴스 데이터 api에서 가져와서 INSERT 문 만들기
    fetch_news_data = PythonOperator(
        task_id = 'fetch_news_data',
        python_callable = fetch_and_prepare_result,
        provide_context = True,
    )

    fetch_nyt_news_data = PythonOperator(
        task_id = 'fetch_nyt_news_data',
        python_callable = fetch_and_prepare_nytimes_result,
        provide_context = True,

    )

    # 검색 Full index 생성하기
    create_news_full_index_1 = SQLExecuteQueryOperator(
        task_id = 'create_news_full_index_1',
        conn_id = 'mysql_saerom',
        sql = sql_create_news_full_index_1,
    )

    # 검색 Full index 생성하기
    create_news_full_index_2 = SQLExecuteQueryOperator(
        task_id = 'create_news_full_index_2',
        conn_id = 'mysql_saerom',
        sql = sql_create_news_full_index_2,
    )
    
    # 뉴스 테이블 만들기
    create_news_table = SQLExecuteQueryOperator(
        task_id = 'create_news_table',
        conn_id = 'mysql_saerom',
        sql = sql_create_news_table,
    )

    # 뉴스 테이블 비우기
    truncate_news_table = SQLExecuteQueryOperator(
        task_id = 'truncate_news_table',
        conn_id = 'mysql_saerom',
        sql = sql_truncate_news_table,
    )

    # 완성된 INSERT문을 실행하여 MYSQL 안에 넣기
    insert_news_data = SQLExecuteQueryOperator(
        task_id = 'insert_news_data',
        conn_id = 'mysql_saerom',
        sql = "{{ task_instance.xcom_pull(task_ids='fetch_news_data', key='insert_news_data') }}",
    )

    # 완성된 INSERT문을 실행하여 MYSQL 안에 넣기
    insert_nyt_news_data = SQLExecuteQueryOperator(
        task_id = 'insert_nyt_news_data',
        conn_id = 'mysql_saerom',
        sql = "{{ task_instance.xcom_pull(task_ids='fetch_nyt_news_data', key='insert_nyt_news_data') }}",
    )

    # 뉴스 업데이트한 시간 업데이트 하기
    update_news_date = SQLExecuteQueryOperator(
        task_id = 'update_news_date',
        conn_id = 'mysql_saerom',
        sql = sql_update_news_date,
    )

    # 작업 순서 설정
    create_news_table >> truncate_news_table >> fetch_news_data >> insert_news_data >> fetch_nyt_news_data >> insert_nyt_news_data >> update_news_date
