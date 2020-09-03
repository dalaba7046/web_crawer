import requests,pandas as pd,logging,lxml,random
from bs4 import BeautifulSoup as bs
from datetime import datetime,timedelta
import time
from opencc import OpenCC
from fake_useragent import UserAgent
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine,update,Table, Column, Integer, String, MetaData, ForeignKey, schema, orm, ext,and_,or_
import configparser
from pathlib import Path
def set_header_user_agent():
    user_agent = UserAgent()
    return user_agent.random
#儲存價格
def data_table(url,today,start_time,con,con2,db_lastest):
    user_agent = set_header_user_agent()
    headers={'User-Agent':user_agent,'Connection':'close'}
    create_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    html=requests.get(url,headers=headers)
    time.sleep(random.uniform(1, 5))
    html.encoding="utf-8"
    soup=bs(html.text,'html.parser')
    #找出網頁po文時間
    p=soup.find('div',class_='td-nei-left').find('div',class_='td-nei-zw').find('div',class_='td-nei-title').find('div',class_='td-time')
    a=[]
    for o in p:
        a.append(o)
    page_time=a[2].replace('\r','').replace('\n','').replace('\u3000','').replace('来源：','')[0:10]
    T=pd.read_html(url)
    '''
    目標是Ｔ[1]表格
    '''
    data=T[1]
    #刪除columns(0,3,4,5)跟標題index[0,1]
    data=data.drop(columns=[3,4,5])
    data=data.drop([0,1])
    #rset index
    data.index=range(len(data))
    #判斷白蒜在哪個位置
    #建個集合
    data1 = pd.DataFrame()
    list0 = []
    list1 = []
    list2 = []
    
    for i in range(len(data)):
        if '杂交白蒜' == data[0][i]:
            list0.append(data.loc[i][0])
            list1.append(data.loc[i][1])
            list2.append(data.loc[i][2])
        else:
            pass
    data1[0]=list0
    data1[1]=list1
    data1[2]=list2
    data = data1
    if '杂交白蒜' in data[0][0]:
        data=data.drop(columns=[0])
        #index重新排序
        data.reset_index(drop=True, inplace=True)
        l=len(data)
        data['low_price']=''
        data['mid_price']=''
        data['avg_price']=''
        data['time']=page_time
        data['create_time']=create_time
        data=data.rename(index=str,columns={1:'variety_name',2:'high_price'})
        for r in range(l):
            if '-' in data['high_price'][r]:
                data['high_price'][r]= data['high_price'][r].split('-')
                data['low_price'][r]=round(float(data['high_price'][r][0])/0.5,2)
                data['high_price'][r]=round(float(data['high_price'][r][1])/0.5,2)
                data['mid_price'][r]=round((float(data['high_price'][r])+float(data['low_price'][r]))/2,2)
                data['avg_price'][r]=round((float(data['high_price'][r])+float(data['low_price'][r]))/2,2)
            else:
                data['high_price'][r]=round(float(data['high_price'][r])/0.5,2)
                data['low_price'][r]=round(float(data['high_price'][r])/0.5,2)
                data['mid_price'][r]=round(float(data['high_price'][r])/0.5,2)
                data['avg_price'][r]=round(float(data['high_price'][r])/0.5,2)
        data=data[['time','variety_name','high_price','mid_price','low_price','avg_price','create_time']]
        logging.info("==============資料新增時間:"+create_time+"====================")
        logging.info(data)
        data.to_sql(name='price_chinese_garlic_market',con=con,if_exists='append',index=False)
        print(data)
        logging.info('已完成'+page_time+'白蒜表格資料新增')
        t_name=data['variety_name'][0]
        t_num=l
        sql="insert into tool_crower_status2 (`time`,`crower_name`,`t_name`,`db_lastest_date`,`crower_date`,`t_num`,`start_time`,`update_time`,`result`,`resource`,`for_table`,`create_time`) values ('{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}');".format(today,'china_market_daily',t_name,db_lastest,today,t_num,start_time,create_time,'T','網頁',"price_chinese_garlic_market",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        con2.execute(sql)
    else:
        logging.info("今日沒有白蒜資料")

#儲存評論
def market_dynamics_table(url,today,start_time,con,con2,db_lastest):
    user_agent = set_header_user_agent()
    headers={'User-Agent':user_agent,'Connection':'close'}
    create_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    #翻譯器
    cc=OpenCC('s2t')
    html=requests.get(url,headers=headers)
    time.sleep(random.uniform(1, 5))
    html.encoding="utf-8"
    soup=bs(html.text,'lxml')
    p=soup.find('div',class_='td-nei-left').find('div',class_='td-nei-zw').find('div',class_='td-nei-title').find('div',class_='td-time')
    #取文章時間

    a=[]
    for o in p:
        a.append(o)
    page_time=a[2].replace('\r','').replace('\n','').replace('\u3000','').replace('来源：','')[0:10]
    Dic={}
    
    s=soup.find('div',class_='gq-pindao').find('div',class_='td-nei-left').find('div',class_='td-nei-zw')
        #當前網頁市場動態內容
    coms=s.find('div',class_='td-nei-content').find('div',class_='content')
    
    
    try:
        if '市场报道' in coms.text:
            
            li=[]
            for c in coms:
                li.append(c)
            
            m=li[-1].replace('\r','').replace('\n','').replace('\u3000','')
            
            market_dynamics=cc.convert(m)
            #市場動態簡轉繁
        else:
            market_dynamics=''
    except:
        logging.exception("=========Exception Logged=============")
    

    Dic={
         "time":page_time,
         "market_dynamics":market_dynamics,
         "create_time":create_time
         }
    
    table2=pd.DataFrame(Dic,index=[1])
    print(table2['time'],table2['market_dynamics'])
    logging.info("==============資料新增時間:"+create_time+"====================")
    table2.to_sql(name='china_market_dynamics',con=con,if_exists='append',index=False)
    logging.info('已完成'+page_time+'市場動態')
    t_name = "大陸白蒜"
    t_num = len(table2)
    sql="insert into tool_crower_status2 (`time`,`crower_name`,`t_name`,`db_lastest_date`,`crower_date`,`t_num`,`start_time`,`update_time`,`result`,`resource`,`for_table`,`create_time`) values ('{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}');".format(today,'china_market_daily',t_name,db_lastest,today,t_num,start_time,create_time,'T','網頁',"china_market_dynamics",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con2.execute(sql)
def main():
    try:
        config_path = str(Path("D:\\Oracle")) + "\\config.ini"
    	config = configparser.ConfigParser()
		config.read(config_path, encoding="utf-8-sig")
		price_db = config.get('section1', 'price_db')
		log_db = config.get('section1', 'log_db')
		price_table = config.get('section1', 'price_table')
		log_table = config.get('section1', 'log_table')
		user = config.get('section1', 'user')
		pw = config.get('section1', 'pw')
		ip = config.get('section1', 'ip')
		port = config.get('section1','port')
        #資料庫連線:
        #價格資料表
        engine = create_engine('mysql+pymysql://'+user+':'+pw+'@'+ip+':'+port+'/'+price_db)
        con = engine.connect()
        #爬蟲狀態資料表
        engine2 = create_engine(('mysql+pymysql://'+user+':'+pw+'@'+ip+':'+port+'/'+log+db+'?charset=utf8')
        con2 = engine2.connect()
        
        #log記錄區
        FROMAT='%(asctime)s-%(levelname)s-> %(message)s'
        log_filename = "D:\\python_crawler\\log\\china_market_daily\\"+datetime.now().strftime("%Y-%m-%d_%H_%M_%S.log")
        logging.getLogger('').handlers=[]
        logging.basicConfig(level=logging.DEBUG,filename=log_filename,format=FROMAT)
        #簡轉繁翻譯器
        cc=OpenCC('s2t')
        
        today=datetime.now().strftime('%Y-%m-%d')
        start_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
        logging.info("================開始爬蟲時間:"+start_time+"==================")
        url='http://www.51garlic.com/hq/list-139.html'
        user_agent = set_header_user_agent()
        headers={'User-Agent':user_agent,'Connection':'close'}
        html=requests.get(url,headers=headers)
        time.sleep(random.uniform(1, 5))
        html.encoding="utf-8"
        soup=bs(html.text,'html.parser')
        create_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
        P=soup.find('div',class_='td-lm-list').find_all('li')
        #資料庫日期check
        #價格
        market_check_date=con.execute('select max(time) from price_chinese_garlic_market').fetchone()[0].strftime('%Y-%m-%d')
        #評論
        dynamics_check_date=con.execute('select max(time) from china_market_dynamics').fetchone()[0].strftime('%Y-%m-%d')
        #今天跟資料庫日期差距
        #價格
        m_days=(datetime.today()-datetime.strptime(market_check_date,"%Y-%m-%d")).days
        #評論
        d_days=(datetime.today()-datetime.strptime(dynamics_check_date,"%Y-%m-%d")).days
        #差距的日期
        the_day_list=[]
        
        for m in range(1,m_days+1):
            the_day=datetime.strptime(today,"%Y-%m-%d")+timedelta(days=-m+1)
            the_day_list.append(the_day.strftime('%Y-%m-%d'))
        logging.info("the_day_list:"+str(the_day_list))
        d_the_day_list=[]
        
        for d in range(1,d_days+1):
            the_day=datetime.strptime(today,"%Y-%m-%d")+timedelta(days=-d+1)
            d_the_day_list.append(the_day.strftime('%Y-%m-%d'))
        logging.info("the_day_list:"+str(d_the_day_list))
        #價格
        if m_days ==1 :
            #評論
            for O in P:
                    #如果資料庫最新一筆日期==今天就新增 
                page_date=datetime.strptime(O.find('span').text,"%Y-%m-%d %H:%M").strftime('%Y-%m-%d')
                
                #如果網頁資料=今天就取出今天連結
                if '金乡' in O.text and '国际交易市场' not in O.text and '农产品交易大厅' not in O.text and today in O.text:
                    logging.info(cc.convert(O.text))
                    link=O.find('a').get('href')
                    
                    #資料庫最新一筆資料
                    
                    print(page_date)
                    if market_check_date == page_date:
                        logging.info("資料庫已有今日資料")
                        logging.info('check_date='+str(market_check_date)+',連結日期='+str(O.find('span').text))
                        pass
                    else:
                        logging.info("資料庫沒有今天資料,準備新增")
                        logging.info('check_date='+str(market_check_date)+',連結日期='+str(O.find('span').text))
                        data_table(link,con)
                        
        elif m_days > 1:
            for day in the_day_list:
                #超過2天從第1頁找到第5頁
                for v in range(1,5):
                    url='http://www.51garlic.com/hq/list-139-'+str(v)+'.html'
                    user_agent = set_header_user_agent()
                    headers={'User-Agent':user_agent,'Connection':'close'}
                    html=requests.get(url,headers=headers)
                    time.sleep(5)
                    html.encoding="utf-8"
                    soup=bs(html.text,'html.parser')
                    create_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
                    P=soup.find('div',class_='td-lm-list').find_all('li')
                    for O in P:
                            if day in O.text and '金乡' in O.text and '国际交易市场' not in O.text and '农产品交易大厅' not in O.text and '下午' not in O.text:
                                
                                link=O.find('a').get('href')
                                
                                data_table(link,today,start_time,con,con2,dynamics_check_date)
                                
        else:
            logging.info("資料庫已有最新資料")
        
        #大蒜評論
        if d_days == 1:
            for I in P:
                #如果網頁資料=今天就取出今天連結
                
                page_date=datetime.strptime(I.find('span').text,"%Y-%m-%d %H:%M").strftime('%Y-%m-%d')
                if '金乡' in I.text and '国际交易市场' not in I.text and '农产品交易大厅' not in I.text and today in I.text:
                    logging.info(cc.convert(I.text))
                    link=I.find('a').get('href')
                    
                    if dynamics_check_date == page_date:
                        logging.info("資料庫已有今日資料")
                        logging.info('dynamics_check_date='+str(dynamics_check_date)+',連結日期='+str(I.find('span').text))
                        pass
                    
                    else:
                        logging.info("資料庫沒有今天資料,準備新增")
                        logging.info('check_date='+str(dynamics_check_date)+',連結日期='+str(I.find('span').text))
                        market_dynamics_table(link,con)
                else:
                    if dynamics_check_date != page_date:
                        logging.info('check_date='+str(dynamics_check_date)+',連結日期='+str(I.find('span').text))
        elif d_days > 1 :
            for day in d_the_day_list:
                 #超過2天從第1頁找到第5頁
                for u in range(1,5):
                    url='http://www.51garlic.com/hq/list-139-'+str(u)+'.html'
                    user_agent = set_header_user_agent()
                    headers={'User-Agent':user_agent,'Connection':'close'}
                    html=requests.get(url,headers=headers)
                    time.sleep(random.uniform(1, 5))
                    html.encoding="utf-8"
                    soup=bs(html.text,'html.parser')
                    create_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
                    P=soup.find('div',class_='td-lm-list').find_all('li')
                    for I in P:
                        if day in I.text and '金乡' in I.text and '国际交易市场' not in I.text and '农产品交易大厅' not in I.text and '下午'  not in I.text:
                            link=I.find('a').get('href')
                            market_dynamics_table(link,today,start_time,con,con2,market_check_date)
        else:
            logging.info("資料庫已有最新資料")
        finish_time=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
        logging.info("====================爬蟲結束,結束時間:"+finish_time+"======================")
        metadata = schema.MetaData(engine2)
        automap = automap_base()
        automap.prepare(engine2, reflect=True)
        session = orm.Session(engine2)
        Table('tool_crower_status2', metadata, autoload=True)
        info = automap.classes['tool_crower_status2']
        #多條件and_filter使用
        re = session.query(info).filter(and_(info.crower_name == 'china_market_daily',info.time == today))
        #更新資料
        re.update({info.finish_time : finish_time}, synchronize_session=False)
        #連線關閉
        session.commit()
        session.close()
        con.close()
        con2.close()
    
    except:
        logging.exception("=========Exception Logged=============")

if __name__ == '__main__':
    main()