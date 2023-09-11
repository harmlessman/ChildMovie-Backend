import requests
import multiprocessing
import json
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

import xml.etree.ElementTree as ET
import firebase_admin
from firebase_admin import credentials, firestore
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


with open('key.json', 'r', encoding='UTF8') as f:
    dic = json.load(f)
    API_KEY = dic['API_KEY']
    FIRESTORE_KEY = dic['FIRESTORE_KEY']
    API_URL = dic['API_URL']


date = dt.now(tz=ZoneInfo("Asia/Seoul"))
d_start = (dt(year=date.year, month=date.month, day=1) - relativedelta(months=1)).strftime("%Y%m%d")
d_end = (dt(year=date.year, month=date.month, day=1) - relativedelta(days=1)).strftime("%Y%m%d")
DB_NAME = (dt(year=date.year, month=date.month, day=1) - relativedelta(months=1)).strftime('%Y-%m')


chrome_options = Options()

chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
url = 'http://ors.kmrb.or.kr/rating/inquiry_mv_list.do'

s = Service(executable_path="/usr/src/chrome/chromedriver")
driver = webdriver.Chrome(service=s, options=chrome_options)
driver.implicitly_wait(10)
driver.get(url)


def get_items(params) -> [dict]:
    response = requests.get(API_URL, params=params)
    root = ET.fromstring(response.content)
    # ET.Element
    items = root.findall('./body/items/item')

    dicts = []

    # ET.Element => dict
    for item in items:
        dic = {}
        for child in item:
            dic[child.tag] = child.text
        dicts.append(dic)

    return dicts


def get_apidata_num(start=d_start, end=d_end):
    params = {
        'ServiceKey': API_KEY,
        'pageNo': 1,
        'numOfRows': 1,
        'stDate': start,
        'edDate': end,
    }
    response = requests.get(API_URL, params=params)
    root = ET.fromstring(response.content)
    return int(root.find('./body/totalCount').text)


def is_app_initialized():
    try:
        firebase_admin.get_app()
        return True
    except ValueError:
        return False


def get_dbdata_num(collection_name):
    if not is_app_initialized():
        cred = credentials.Certificate(FIRESTORE_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    document = db.collection(collection_name).get()
    db.close()
    return len(document)


def add_descriptive_content(dicts):
    # with multiprocessing.Pool(processes=4) as pool:
    #     results = pool.map(get_descriptive_content, dicts)
    results = []
    for i in dicts:
        a = get_descriptive_content(i)
        results.append(a)

    return results


def insert_data(dicts, collection_name):
    cred = credentials.Certificate(FIRESTORE_KEY)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    for dic in dicts:
        document = db.collection(collection_name).document()
        document.set(dic)

    db.close()

    return 1


def get_descriptive_content(dic):
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


def validate_db():
    api_data_num = get_apidata_num()

    db_data_num = get_dbdata_num(DB_NAME)

    print(f'date = {DB_NAME} => api_num : {api_data_num}, db_num : {db_data_num}')
    if api_data_num == db_data_num:
        return True
    else:
        return False


def update():
    params = {
        'serviceKey': API_KEY,
        'pageNo': '1',
        'numOfRows': '9999',
        'stDate': d_start,
        'edDate': d_end,
    }

    data = add_descriptive_content(get_items(params))
    insert_data(data, DB_NAME)

    print('DB updated!')

    if validate_db():
        print(f'{DB_NAME} : DB validated')
    else:
        print(f'{DB_NAME} : DB invalidated')


if __name__ == '__main__':
    update()
