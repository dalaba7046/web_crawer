import requests,pandas as pd,csv,time as ti,random
from bs4 import BeautifulSoup as bs
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import urllib.parse
from fake_useragent import UserAgent
def set_header_user_agent():
    user_agent = UserAgent()
    return user_agent.random
def data_set(st_no,st_na,date):
    sleeptime=random.randint(5,10)
    url='https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station='+str(st_no)+'&stname='+urllib.parse.quote(urllib.parse.quote(st_na))+'&datepicker='+date
    user_agent = set_header_user_agent()
    print(url)
    res = requests.get(url,verify=False,headers={ 'user-agent': user_agent })
    ti.sleep(sleeptime)
    html=res.text
    
    soup=bs(html,'html.parser')
    stno=st_no
    
    if '本段時間區間內無觀測資料！' in soup.text:
        print('本段時間區間內無觀測資料！')
        pass
    else:
        if st_no=='C0V250' or st_no=='C0Z160':
            for tr_index in range(4,27):
                tr_data=soup.find_all('tr')[tr_index]
                td_data=tr_data.find_all('td')
                if tr_data.find('td').text=='24':
                    time=date+"00:00:00"
                    time=datetime.strptime(time,'%Y-%m-%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
                else:
                    time=date+tr_data.find('td').text+":00:00"
                    time=datetime.strptime(time,'%Y-%m-%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
            
                for td_index in range(1,17):
                    td_data[td_index]=td_data[td_index].get_text(strip=True)
                    if(td_data[td_index]== '...' or td_data[td_index]== '/' or td_data[td_index]== 'X'  or  td_data[td_index]== 'T' or td_data[td_index]== 'V'):
                        td_data[td_index]=-9999
                    elif(td_data[td_index]==''):
                        td_data[td_index]='NULL'
                create_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sql = "insert into cwb_data_hr (stno,time,PS01,PS02,TX01,TX05,RH01,WD01,WD02,WD05,WD06,PP01,PP02,SS01,SS02,VS01,uvi,CD11,create_time) VALUES ('%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s');"%(stno,time,td_data[1],td_data[2],td_data[3],td_data[4],td_data[5],td_data[6],td_data[7],td_data[8],td_data[9],td_data[10],td_data[11],td_data[12],td_data[13],td_data[14],td_data[15],td_data[16],create_time)
                print(st_no+'，'+time+'有資料，開始新增至資料庫')
                con2.execute(sql)
        
        else: 
            for tr_index in range(4,28):
                tr_data=soup.find_all('tr')[tr_index]
                td_data=tr_data.find_all('td')
                if tr_data.find('td').text=='24':
                    time=date+"00:00:00"
                    time=datetime.strptime(time,'%Y-%m-%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
                else:
                    time=date+tr_data.find('td').text+":00:00"
                    time=datetime.strptime(time,'%Y-%m-%d%H:%M:%S').strftime("%Y-%m-%d %H:%M:%S")
            
                for td_index in range(1,17):
                    td_data[td_index]=td_data[td_index].get_text(strip=True)
                    if(td_data[td_index]== '...' or td_data[td_index]== '/' or td_data[td_index]== 'X'  or  td_data[td_index]== 'T' or td_data[td_index]== 'V'):
                        td_data[td_index]=-9999
                    elif(td_data[td_index]==''):
                        td_data[td_index]='NULL'
                create_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sql2 = "insert into cwb_data_hr (stno,time,PS01,PS02,TX01,TX05,RH01,WD01,WD02,WD05,WD06,PP01,PP02,SS01,SS02,VS01,uvi,CD11,create_time) VALUES ('%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s');"%(stno,time,td_data[1],td_data[2],td_data[3],td_data[4],td_data[5],td_data[6],td_data[7],td_data[8],td_data[9],td_data[10],td_data[11],td_data[12],td_data[13],td_data[14],td_data[15],td_data[16],create_time)
                print(sql2)
                print(st_no+'，'+time+'有資料，開始新增至資料庫')
                con2.execute(sql2)


engine=create_engine('mysql+pymysql://adminer:Pn107@192.168.1.101:3306/util?charset=utf8')
con = engine.connect()

engine2=create_engine('mysql+pymysql://adminer:Pn107@192.168.1.101:3306/cwb?charset=utf8')
con2=engine2.connect()

sql='select locationname,station_id from tool_station_info'
sta_list=con.execute(sql).fetchall()

st_na=[]
st_no=[]

for i in range(len(sta_list)):
    st_na.append(sta_list[i][0])
    st_no.append(sta_list[i][1])

#將時間及氣象站帶入
# for i in range(4,5):
#     for j in range(18,19):
#         #如果日期<10，就在數字前+個0 EX
#         if j<10 :
#             date='2020-0'+str(i)+'-0'+str(j)
today=datetime.today()
# date=today+timedelta(days=-2)
# date=date.strftime('%Y-%m-%d')
date10=today+timedelta(days=-6)
date11=today+timedelta(days=-5)
date12=today+timedelta(days=-4)
date10=date10.strftime('%Y-%m-%d')
date11=date11.strftime('%Y-%m-%d')
date12=date12.strftime('%Y-%m-%d')

dates=[date10,date11,date12]

#當前資料庫具備的氣象站
li=[]
#判斷當前資料庫中已有的氣象站並排除掉
for date in dates:
    check_sql="select stno from cwb_data_hr where time like '%%"+date+"%%'"+" group by stno"
    result=con2.execute(check_sql).fetchall()
    print(result)
    for a in range(len(result)):
        li.append(result[a][0])
    remove_st_no=[]
    remove_st_na=[]
    
    for b in range(len(st_no)):
        for c in range(len(li)):
            if li[c]==st_no[b]:
                print(st_no[b],st_na[b],c,b)
                remove_st_no.append(st_no[b])
                remove_st_na.append(st_na[b])
            else:
                pass
    for removestno in remove_st_no:
        st_no.remove(removestno)
    for removestna in remove_st_na:
        st_na.remove(removestna)
    
    for k,l in zip(st_no,st_na):
        print(k,l,date)
        data_set(k,l,date)
        
    

con.close()
con2.close()