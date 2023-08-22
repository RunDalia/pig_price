# 최종 파일

from bs4 import BeautifulSoup
import time, os
from datetime import datetime, timedelta, date
import datetime as dt
import requests
from loguru import logger
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
import configparser


def get_data():
    today = datetime.today()
    yesterday = (today - timedelta(1)).strftime('%Y%m%d')

    url = 'https://www.ekapepia.com/priceStat/forwardTrendStatus.do'
    header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
    param = {
        'menuId': 'menu100249',
        'searchStartDate': yesterday,
        'searchEndDate': yesterday,
    }

    res = requests.post(url, headers=header, data=param)

    soup = BeautifulSoup(res.content, 'lxml')
    tbody = soup.find('tbody')
    tr = tbody.find_all('tr')
    tr.pop(0)

    all_data_list = []
    for i in tr:
        pre_df = dict()
        try:
            date = i.find('th', attrs={'scope': 'col'}).text
            date = dt.datetime.strptime(date, '%m월 %d일').strftime('%m%d')
            month = date[0:2]
            if '0' in month:
                date = '2023' + date
            else:
                date = '2022' + date
            pre_df['date'] = date
            # pre_df['insert_dttm'] = str(datetime.now())
            td_list = i.find_all('span', attrs={'class':'mr5'})
            for index, td in enumerate(td_list):
                if index == 8:
                    jdg = td.text.replace(',', '')
                    pre_df['jdgCnt'] = jdg
                elif index == 9:
                    trd = td.text.replace(',', '')
                    pre_df['trdVol'] = trd
            all_data_list.append(pre_df)
        except:
            pass

    for data in all_data_list:
        if data['jdgCnt'] == '':
            all_data_list.remove(data)

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
            tableName = 'tb_day_trade'
            columnList = ['date', 'jdgCnt', 'trdVol']
            valsStr = (len(columnList) * '%s,')[:-1]
            all_data = get_data()
            for row in all_data:
                cur.execute(f'''
                INSERT INTO {tableName} ({','.join(columnList)},insert_dttm)
                VALUES ({valsStr}, NOW())
                ON DUPLICATE KEY UPDATE 
                    jdgCnt = VALUES(jdgCnt),
                    trdVol = VALUES(trdVol),
                    insert_dttm = VALUES(insert_dttm);
                ''', (row['date'], row['jdgCnt'], row['trdVol']))
                logger.info(row)
            logger.success(f'{tableName} DB insert success')
            conn.commit()
        return
    except Exception as ee:
        logger.error(f'DB insert Error: {ee}')


def main(config):
    sched = BackgroundScheduler()
    # test용
    # sched.add_job(insert_mysql, 'cron', second='*/5', name='tb_day_trade', args=[config])
    # 실제 자동화 / 매일 오전 10시 20분 실행 예정 
    sched.add_job(insert_mysql, 'cron', hour='10', minute='20', name='tb_day_trade', args=[config])
    sched.start()

    while True:
        time.sleep(1)


if __name__ == '__main__':

    config = configparser.ConfigParser(os.environ)
    config.read('./config/config.cfg')

    # 로그 남기기
    logger.add(
        sink=config['log']['directory'] + '/tb_day_trade.log',
        enqueue=True,
        retention=int(config['log']['retention']),
        rotation=config['log']['rotation'],
        level=config['log']['level'].upper()
    )
    main(config)