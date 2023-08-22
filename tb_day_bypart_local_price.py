# 최종 파일

import time, os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import datetime as dt
import requests
import pymysql
from loguru import logger
from apscheduler.schedulers.background import BackgroundScheduler
import configparser


def get_data():
    today = datetime.today()
    yesterday = (today - timedelta(1)).strftime('%Y%m%d')

    url = 'https://www.ekapepia.com/priceStat/distrPricePork.do'
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
    param = {
        'menuId': 'menu100034',
        'searchStartDate': yesterday,
        'searchEndDate': yesterday,
        'radioChk': 'month',
    }
    res = requests.post(url, headers = header, data = param)

    soup = BeautifulSoup(res.content, 'lxml')
    tbody = soup.find('tbody')
    tr = tbody.find_all('tr')

    all_data_list = []

    for i in tr:
        pre_df = dict()
        try:
            date = i.find('th', attrs={'scope': 'row'}).get_text()
            pre_df['date'] = dt.datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
            data = i.find_all('span', attrs={'class': 'mr5'})
            for index, content in enumerate(data):
                if index % 4 == 0:
                    sanji = content.get_text()
                    pre_df['farm_price'] = int(sanji)
                elif index % 4 == 1:
                    avg = content.get_text().replace(',', '')
                    pre_df['avg'] = int(avg)
                elif index % 4 == 2:
                    first = content.get_text().replace(',', '')
                    pre_df['grade_1'] = int(first)
                else:
                    price = content.get_text().replace(',', '')
                    pre_df['pork_belly'] = int(price)
            # pre_df['insert_dttm'] = str(datetime.now())
            all_data_list.append(pre_df)
        except Exception as e:
            print(e)
    return all_data_list


def conn_maria(config):
    try:
        conn = pymysql.connect(host=config['mariadb']['host'],
                               user=config['mariadb']['user'],
                               password=config['mariadb']['password'],
                               db=config['mariadb']['db'],
                               charset=config['mariadb']['charset'])
        logger.success('DB connection success')
        return conn
    except Exception as ee:
        logger.error(f'DB connection Error: {ee}')


def insert_mysql(config):
    try:
        conn = conn_maria(config)
        with conn.cursor() as cur:
            tableName = 'tb_day_bypart_local_price'
            columnList = ['date', 'farm_price', 'avg','grade_1','pork_belly']
            valsStr = (len(columnList) * '%s,')[:-1]
            all_data = get_data()
            for row in all_data:
                cur.execute(f'''
                INSERT INTO {tableName} ({','.join(columnList)}, insert_dttm) 
                VALUES ({valsStr}, NOW())
                ON DUPLICATE KEY UPDATE
                    farm_price = VALUES(farm_price),
                    avg = VALUES(avg),
                    grade_1 = VALUES(grade_1),
                    pork_belly = VALUES(pork_belly),
                    insert_dttm = VALUES(insert_dttm);
                ''', (row['date'], row['farm_price'], row['avg'], row['grade_1'], row['pork_belly']))
                logger.info(row)
            logger.success(f'{tableName} DB insert success')
            conn.commit()
        return
    except Exception as ee:
        logger.error(f'DB insert Error: {ee}')


def main(config):
    sched = BackgroundScheduler()
    # test용
    # sched.add_job(insert_mysql, 'cron', second='*/5', name='tb_day_bypart_local_price', args=[config])
    # 실제 자동화 / 매일 오전 10시 20분 실행 
    sched.add_job(insert_mysql, 'cron', hour='10', minute='20', name='tb_day_bypart_local_price', args=[config])
    sched.start()

    while True:
        time.sleep(1)


if __name__ == '__main__':

    config = configparser.ConfigParser(os.environ)
    config.read('./config/config.cfg')

    # 로그 남기기
    logger.add(
        sink= config['log']['directory'] + '/tb_day_bypart_local_price.log',
        enqueue=True,
        retention=int(config['log']['retention']),
        rotation=config['log']['rotation'],
        level=config['log']['level'].upper()
    )
    main(config)