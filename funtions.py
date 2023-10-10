import requests
import json
import multiprocessing
import sqlite3
from pathlib import Path

import xml.etree.ElementTree as ET
import firebase_admin
from firebase_admin import credentials, firestore
from selenium.webdriver.common.by import By


global driver

count_18 = 0


def set_driver(my_driver):
    global driver
    driver = my_driver


def get_items(api_url, params) -> [dict]:
    response = requests.get(api_url, params=params)
    root = ET.fromstring(response.content)
    # ET.Element
    items = root.findall('./body/items/item')

    dicts = []
    global count_18

    # ET.Element => dict
    for item in items:
        dic = {}
        for child in item:
            dic[child.tag] = child.text

        # 성인영상물일 경우 dicts에 추가하지 않음
        if (dic.get('gradeName') == '청소년관람불가' or dic.get('gradeName') == '제한상영가') and dic.get('coreHarmRsn') == '선정성':
            count_18 += 1
            continue

        dicts.append(dic)

    return dicts


def get_apidata_num(api_url, api_key, start, end):
    params = {
        'ServiceKey': api_key,
        'pageNo': 1,
        'numOfRows': 1,
        'stDate': start,
        'edDate': end,
    }
    response = requests.get(api_url, params=params)
    root = ET.fromstring(response.content)
    return int(root.find('./body/totalCount').text)


def is_app_initialized():
    try:
        firebase_admin.get_app()
        return True
    except ValueError:
        return False


def get_dbdata_num(collection_id, firestore_key):
    if not is_app_initialized():
        cred = credentials.Certificate(firestore_key)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    documents_num = db.collection(collection_id).count().get()

    return documents_num[0][0].value


def add_descriptive_content(dicts, multiprocess: bool = False):
    results = []

    if multiprocess:
        with multiprocessing.Pool(processes=4) as pool:
            results = pool.map(get_descriptive_content, dicts)
    else:
        for i in dicts:
            dic = get_descriptive_content(i)
            if len(dic) > 0:
                results.append(dic)
    return results


def insert_data(dicts, collection_id, firestore_key):
    cred = credentials.Certificate(firestore_key)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    for dic in dicts:
        document = db.collection(collection_id).document()
        document.set(dic)

    return 1


def get_descriptive_content(dic):
    global driver

    driver.find_element(By.NAME, 'rt_no').clear()
    driver.find_element(By.NAME, 'mv_use_title').clear()
    driver.find_element(By.NAME, 'rt_no').send_keys(dic['rtNo'])
    driver.find_element(By.NAME, 'mv_use_title').send_keys(dic['useTitle'])
    driver.find_element(By.CLASS_NAME, 'btn_bottom_search').click()

    driver.find_element(By.XPATH, '/html/body/section[2]/form/div/div[3]/table/tbody/tr/td[2]/a').click()
    xpaths = driver.find_elements(By.XPATH, '/html/body/section[2]/div/table/tbody/tr')

    for xpath in xpaths:
        if xpath.text.split('\n')[0] == '줄거리,':
            if len(xpath.text.split('\n')) < 3:
                driver.find_element(By.XPATH, '/html/body/section[2]/div/div/input').click()
                dic['descriptive_content'] = ''

            else:
                text = xpath.text.split('\n')[-1]
                driver.find_element(By.XPATH, '/html/body/section[2]/div/div/input').click()
                dic['descriptive_content'] = text

            return dic

    # 만약 다른페이지로 넘어가서 descriptive_content를 가져오지 못했을 경우에는 원래 페이지로 돌아간다.
    # (성인 영상물인 경우 인증페이지로 넘어감. get_items()의 필터로 못 걸러 냈을 때를 대비하는 코드)
    global count_18
    count_18 += 1
    driver.get('http://ors.kmrb.or.kr/rating/inquiry_mv_list.do')
    return {}


def update(
        key_path,
        start_date,
        end_date,
        collection_id,
        my_driver
):
    """
    Update movie information

    Args:
        key_path (str): ~
        start_date (str): ~
        end_date (str): ~
        collection_id (str): ~
        my_driver (WebDriver): ~

    Return:

    """
    with open(key_path, 'r', encoding='UTF8') as f:
        dic = json.load(f)
        api_key = dic['API_KEY']
        firestore_key = dic['FIRESTORE_KEY']
        api_url = dic['API_URL']

    set_driver(my_driver)

    params = {
        'serviceKey': api_key,
        'pageNo': '1',
        'numOfRows': '9999',
        'stDate': start_date,
        'edDate': end_date,
    }

    data = add_descriptive_content(get_items(api_url, params))
    insert_data(data, collection_id, firestore_key)

    api_data_num = get_apidata_num(api_url, api_key, start_date, end_date)

    db_data_num = get_dbdata_num(collection_id, firestore_key)

    print(f'collection name : {collection_id}')
    print(f'Date : {start_date} ~ {end_date}')
    print(f'api_num : {api_data_num}\ndb_num : {db_data_num}\ncount_18 : {count_18}')

    if api_data_num == db_data_num + count_18:
        print('Success')
        return True
    else:
        print('Failure')
        return False


def combine_collection(firestore_key, target_collection_id: str = 'movie_db'):
    if not is_app_initialized():
        cred = credentials.Certificate(firestore_key)
        firebase_admin.initialize_app(cred)

    doc_count = 0

    db = firestore.client()
    collection_list = db.collections()

    for collection in collection_list:
        collection_id = collection.id
        if collection_id == target_collection_id or not collection_id.isdigit():
            continue

        documents = collection.get()
        for document in documents:
            db.collection(target_collection_id).document().set(document.to_dict())
            doc_count += 1

    return doc_count


def remove_adult_movie(firestore_key, collection_id):
    if not is_app_initialized():
        cred = credentials.Certificate(firestore_key)
        firebase_admin.initialize_app(cred)

    deleted_count = 0

    db = firestore.client()
    documents = db.collection(collection_id).list_documents()
    for document in documents:
        dic = document.get().to_dict()
        if (dic.get('gradeName') == '청소년관람불가' or dic.get('gradeName') == '제한상영가') and dic.get('coreHarmRsn') == '선정성':
            document.delete()
            deleted_count += 1

    return deleted_count


def create_db_file(firestore_key, file_path: str = 'db.db', collection_id: str = 'movie_db'):
    if Path(file_path).exists():
        print('이미 같은 이름의 .db파일이 존재합니다!')
        return

    movie_list = []
    default_dic = {
        'aplcName': None,
        'coreHarmRsn': None,
        'descriptive_content': None,
        'direName': None,
        'direNatnlName': None,
        'gradeName': None,
        'leadaName': None,
        'mvAssoName': None,
        'oriTitle': None,
        'prodYear': None,
        'prodcName': None,
        'prodcNatnlName': None,
        'rtCoreHarmRsnNm': None,
        'rtDate': None,
        'rtNo': None,
        'rtStdName1': None,
        'rtStdName2': None,
        'rtStdName3': None,
        'rtStdName4': None,
        'rtStdName5': None,
        'rtStdName6': None,
        'rtStdName7': None,
        'screTime': None,
        'stadCont': None,
        'suppaName': None,
        'useTitle': None,
        'workCont': None,
    }
    table_name = "movies"

    create_table_query =f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
      id INTEGER PRIMARY KEY,
      aplcName TEXT,
      coreHarmRsn TEXT,
      descriptive_content TEXT,
      direName TEXT,
      direNatnlName TEXT,
      gradeName TEXT,
      leadaName TEXT,
      mvAssoName TEXT,
      oriTitle TEXT,
      prodYear TEXT,
      prodcName TEXT,
      prodcNatnlName TEXT,
      rtCoreHarmRsnNm TEXT,
      rtDate TEXT,
      rtNo TEXT,
      rtStdName1 TEXT,
      rtStdName2 TEXT,
      rtStdName3 TEXT,
      rtStdName4 TEXT,
      rtStdName5 TEXT,
      rtStdName6 TEXT,
      rtStdName7 TEXT,
      screTime TEXT,
      stadCont TEXT,
      suppaName TEXT,
      useTitle TEXT,
      workCont TEXT
      )
    """

    columns = ', '.join(default_dic.keys())
    placeholders = ':' + ', :'.join(default_dic.keys())
    insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'

    if not is_app_initialized():
        cred = credentials.Certificate(firestore_key)
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    documents = db.collection(collection_id).get()

    for document in documents:
        movie_list.append({**default_dic, **document.to_dict()})

    con = sqlite3.connect(file_path)

    cur = con.cursor()
    cur.execute(create_table_query)

    cur.executemany(insert_query, movie_list)

    con.commit()
    con.close()

    print(f'{file_path}에 저장되었습니다!')
    return




