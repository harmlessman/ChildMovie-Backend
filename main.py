from funtions import update
from zoneinfo import ZoneInfo
from datetime import datetime as dt, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# for cloud run
chrome_options = Options()

chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
url = 'http://ors.kmrb.or.kr/rating/inquiry_mv_list.do'

s = Service(executable_path="/usr/src/chrome/chromedriver")
driver = webdriver.Chrome(service=s, options=chrome_options)
driver.implicitly_wait(10)
driver.get(url)

# for local
# chrome_options = Options()
# chrome_options.add_experimental_option("detach", True)
# chrome_options.add_argument('headless')
# url = 'http://ors.kmrb.or.kr/rating/inquiry_mv_list.do'
#
# driver = webdriver.Chrome(options=chrome_options)
#
# driver.get(url)

if __name__ == '__main__':
    # 7일전 데이터를 가져옴 (ors 데이터 업로드 딜레이를 감안)
    date = (dt.now(tz=ZoneInfo("Asia/Seoul")) - timedelta(days=7)).strftime('%Y%m%d')

    update(
        key_path='keys.json',
        start_date=date,
        end_date=date,
        collection_name=date,
        my_driver=driver,
    )