import requests
import json
import multiprocessing

import xml.etree.ElementTree as ET
import firebase_admin
from firebase_admin import credentials, firestore
from selenium.webdriver.common.by import By


global driver


def set_driver(my_driver):
    global driver
    driver = my_driver


def get_items(api_url, params) -> [dict]:
    response = requests.get(api_url, params=params)
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


def get_dbdata_num(collection_name, firestore_key):
    if not is_app_initialized():
        cred = credentials.Certificate(firestore_key)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    document = db.collection(collection_name).get()
    db.close()
    return len(document)


def add_descriptive_content (dicts, multiprocess: bool = False):
    results = []

    if multiprocess:
        with multiprocessing.Pool(processes=4) as pool:
            results = pool.map(get_descriptive_content, dicts)
    else:
        for i in dicts:
            a = get_descriptive_content(i)
            results.append(a)

    return results


def insert_data(dicts, collection_name, firestore_key):
    cred = credentials.Certificate(firestore_key)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    for dic in dicts:
        document = db.collection(collection_name).document()
        document.set(dic)

    db.close()

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


def validate_db():
    api_data_num = get_apidata_num()

    db_data_num = get_dbdata_num()

    print(f'date =  => api_num : {api_data_num}, db_num : {db_data_num}')
    if api_data_num == db_data_num:
        return True
    else:
        return False


def update(
        key_path,
        start_date,
        end_date,
        collection_name,
        my_driver
):
    """
    Update movie information

    Args:
        key_path (str): ~
        start_date (str): ~
        end_date (str): ~
        collection_name (str): ~
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
    insert_data(data, collection_name, firestore_key)



