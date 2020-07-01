import time,requests,os,random,pandas as pd
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup as bs
from selenium.common.exceptions import NoSuchElementException,NoAlertPresentException,ElementNotInteractableException
from selenium.webdriver.support.ui import Select
from sqlalchemy import create_engine
from fake_useragent import UserAgent
from datetime import datetime

engine=create_engine('mysql+pymysql://adminer:Pn107@192.168.1.101:3306/issue_tool?charset=utf8')
con = engine.connect()
#建立虛擬使用者
def set_header_user_agent():
    user_agent = UserAgent()
    return user_agent.random

user_agent=set_header_user_agent()
headers={ 'user-agent': user_agent }
url='https://newplant.afa.gov.tw/'

sleeptime=random.randint(10,15)
options = webdriver.ChromeOptions()
prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': '/Users/liuyukun/work/'}
options.add_experimental_option('prefs', prefs)
options.add_argument("Connection=close")
options.add_argument('user_agent='+user_agent)
driver=webdriver.Chrome(chrome_options=options,executable_path='/Users/liuyukun/python/chromedriver')
driver.get(url)
time.sleep(5)
#soup = bs(driver.page_source, 'html.parser')
driver.switch_to_frame('main')
time.sleep(5)
driver.find_element_by_xpath('//*[@id="hiddownloadform"]/table/tbody/tr[2]/td/div[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[2]/td[2]/a').click()
time.sleep(7)


#瀏覽器找到分類的元素
li=driver.find_elements_by_tag_name('a')[2:]
#將元素內容添加到分類list
classification=[]
for i in range(len(li)):
    classification.append(li[i].text)
#點擊全部展開
driver.find_elements_by_tag_name('a')[0].click()
time.sleep(3)

#作物分類元素
crop_li=driver.find_elements_by_class_name('GridCommonRow3Open')
#取得所有分類連結及名稱

#作物分類
crop=[]
#新品種名稱
new_cropname=[]
#新品種申請人
new_applicationer=[]
#元素總數
crop_len=len(crop_li)
#字串取出
crop_list=[]
for i in range(crop_len):
    crop_list.append(crop_li[i].text)

for j in range(crop_len):
    if '\n' in crop_list[j]:
        print(crop_list[j].split('\n'))
        #crop_list[j].split('\n')
        for k in range(len(crop_list[j].split('\n')[1:])):
            
            crop.append(crop_list[j].split('\n')[0])
            new_cropname.append(crop_list[j].split('\n')[1:][k].split(' ')[0])
            new_applicationer.append(crop_list[j].split('\n')[1:][k].split(' ')[1])
    else:
        print(crop_list[j].split('\n'))
        crop.append(crop_list[j].split('\n')[0])
        new_cropname.append('')
        new_applicationer.append('')


'''
蔬菜從crop[0]到crop[58]
花卉從crop[59]到crop[133]
果樹從crop[134]到crop[171]
糧食作物從crop[172]到crop[174]
林木從crop[175]到crop[176]
其他從crop[177]到crop[194]
菇蕈從crop[195]到crop[200]
'''
df=pd.DataFrame()

df['crop']=crop
df['new_cropname']=new_cropname
df['new_applicationer']=new_applicationer
df['create_time']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
for l in range(len(df['crop'])):
    if '落葵' in df['crop'][l]:
        print('落葵在'+str(l))
        df['classification'][0:194]=classification[0]
    elif '大岩桐' in df['crop'][l]:
        print('大岩桐'+str(l))
        df['classification'][195:2308]=classification[1]
    elif '百香果' in df['crop'][l]:
        print('百香果'+str(l))
        df['classification'][2309:2441]=classification[2]
    elif '玉米' in df['crop'][l]:
         print('玉米'+str(l))
         df['classification'][2442:2507]=classification[3]
    elif '土肉桂' in df['crop'][l]:
         print('土肉桂'+str(l))
         df['classification'][2508:2511]=classification[4]
    elif '高粱' in df['crop'][l]:
         print('高粱'+str(l))
         df['classification'][2512:2554]=classification[5]
    elif '金針菇' in df['crop'][l]:
         print('金針菇'+str(l))
         df['classification'][2555:2562]=classification[6]

df.to_sql(name='newplant',con=con,if_exists='append',index=False)