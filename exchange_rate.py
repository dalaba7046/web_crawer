import twder,pandas as pd,logging
from sqlalchemy import create_engine
from datetime import datetime,date
import configparser
from pathlib import Path

def currency_len(currency,today,con):
    sql=f"select date from exchenge_rate where currency='{currency}' and date='{today}'"
    data=con.execute(sql).fetchall()
    
    return data
def main():
    today=date.today().strftime("%Y-%m-%d")
    # FROMAT='%(asctime)s-%(levelname)s-> %(message)s'
    # log_filename = "D:/python_crawler/log/exchange_rate/"+datetime.now().strftime("%Y-%m-%d_%H_%M_%S.log")
    #log_filename = "/Users/liuyukun/work/python_crawler/log/exchange_rate/"+datetime.now().strftime("%Y-%m-%d_%H_%M_%S.log")
    # logging.getLogger('').handlers=[]
    # logging.basicConfig(level=logging.DEBUG,filename=log_filename,format=FROMAT)

    
    config_path = str(Path("D:\\Oracle")) + "\\config.ini"
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8-sig")
    price_db = config.get('section1', 'price_db')
    log_db = config.get('section1', 'log_db')
    user = config.get('section1', 'user')
    pw = config.get('section1', 'pw')
    ip = config.get('section1', 'ip')
    port = config.get('section1','port')
    log = config.get('section1','log_db')
    
    #資料庫連線:
    #價格資料表
    engine = create_engine('mysql+pymysql://'+user+':'+pw+'@'+ip+':'+port+'/'+price_db)
    con = engine.connect()
    #爬蟲狀態資料表
    engine2 = create_engine('mysql+pymysql://'+user+':'+pw+'@'+ip+':'+port+'/'+log_db+'?charset=utf8')
    con2 = engine2.connect()
        
    try:
        
        #資料list
        currencies=['USD','CNY','JPY']
        data_list=[]
        #開始時間
        start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #今天
        
        #將貨幣帶入twder套件twder.now()
        for cu in currencies:
            data_list.append(twder.now(cu))
        #logging.info(data_list)
        try:
            #貨幣日期判斷是否最新
            for data,cc in zip(data_list,currencies):
                
                check_db_last_date=f'select max(date) from exchenge_rate where currency="{cc}"'
                #資料庫最後一天
                last_date=con.execute(check_db_last_date).fetchone()[0].strftime('%Y-%m-%d')
                #爬蟲日期
                data_date=datetime.strptime(data[0],"%Y/%m/%d %H:%M").strftime("%Y-%m-%d")
                logging.info(cc+"資料庫最後一天為："+last_date+"。爬蟲日期為："+data_date)
                if data_date != last_date:
                    #logging.info(cc+"未更新至最新資料,資料整理中")
                    
                    time = datetime.strptime(data[0],"%Y/%m/%d %H:%M").strftime("%Y-%m-%d")
                    
                    cash_buy = float(data[1])
                    bank_buy = float(data[3])
                    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df = pd.DataFrame()
                    
                    dic={
                         'date':time,
                         'currency':cc,
                         'cash_buy':cash_buy,
                         'bank_buy':bank_buy,
                         'create_time':create_time
                         }
                    df['date']=dic['date']
                    df['currency']=dic['currency']
                    df['cash_buy']=dic['cash_buy']
                    df['bank_buy']=dic['bank_buy']
                    df['create_time']=dic['create_time']

                    df = df.append(dic,ignore_index=True)
                    
                    df.to_sql(name='exchenge_rate',con=con,if_exists='append',index=False)
                    finish_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    t_num = len(currency_len(cc,today,con))
                    data_group = [today,'exchange_rate',cc,last_date,time,t_num,start_time,create_time,finish_time,'T','python套件','exchenge_rate',datetime.now().strftime("%Y-%m-%d %H:%M:%S")] 
                    sql = "insert into tool_crower_status2 (`time`,`crower_name`,`t_name`,`db_lastest_date`,`crower_date`,`t_num`,`start_time`,`update_time`,`finish_time`,`result`,`resource`,`for_table`,`create_time`) values('{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}','{}')".format(data_group[0],data_group[1],data_group[2],data_group[3],data_group[4],data_group[5],data_group[6],data_group[7],data_group[8],data_group[9],data_group[10],data_group[11],data_group[12])
                    con2.execute(sql)
                else:
                    #logging.info(cc+"資料庫已有最新檔案")
                    finish_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    data_create_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    t_num=0
                    db_last_date=con.execute(check_db_last_date).fetchone()[0].strftime('%Y-%m-%d')
                    crower_date=currency_len(cc,today,con)[0][0].strftime("%Y-%m-%d")
                    data_group = [today,'exchange_rate',cc,db_last_date,crower_date,t_num,start_time,data_create_time,finish_time,'T','python套件','exchenge_rate',data_create_time] 
                    logging.info(data_group)
                    sql="insert into tool_crower_status2 (`time`,`crower_name`,`t_name`,`db_lastest_date`,`crower_date`,`t_num`,`start_time`,`update_time`,`finish_time`,`result`,`resource`,`for_table`,`create_time`) values('{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}','{}')".format(data_group[0],data_group[1],data_group[2],data_group[3],data_group[4],data_group[5],data_group[6],data_group[7],data_group[8],data_group[9],data_group[10],data_group[11],data_group[12])
                    logging.info(sql)
                    con2.execute(sql)
        except Exception as ee:
            logging.exception("=========Exception Logged=============")
            ee=str(ee)
            create_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_sql="insert tool_crower_status2 (`time`,`crower_name`,`crower_date`,`result`,`resource`,`for_table`,`error_message`,`create_time`) values('{}','{}','{}','{}','{}','{}','{}','{}')".format(today,'exchange_rate',today,'F','網頁','exchenge_rate',ee,create_time)
            con2.execute(error_sql)
            con.close()
            con2.close()
     
        con.close()
        con2.close()
    
    except Exception as ee:
        logging.exception("=========Exception Logged=============")
        ee=str(ee)
        create_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_sql="insert tool_crower_status2 (`time`,`crower_name`,`crower_date`,`result`,`resource`,`for_table`,`error_message`,`create_time`) values('{}','{}','{}','{}','{}','{}','{}')".format(today,'exchange_rate',today,'F','網頁','exchenge_rate',ee,create_time)
        con2.execute(error_sql)
        con.close()
        con2.close()
    
if __name__== "__main__":
    main()
