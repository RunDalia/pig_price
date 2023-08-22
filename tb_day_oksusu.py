# 최종 파일

import time, os
import requests
from loguru import logger
import pymysql
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
import configparser


url = 'https://kr.investing.com/commodities/us-corn-historical-data'
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}


def get_one_data():
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.content, 'lxml')

    # 데이터가 있는 tbody만 추출
    div = soup.find('div', attrs = {'class':'border border-main'})
    tbody = div.find('tbody')

    # 매일 하나의 데이터만 가져올 것이므로 가장 위의 tr만 가져오기
    tr = tbody.find_all('tr')[1]

    all_data_list = []
    for i in tr:
        data_text = i.get_text().replace('- ', '').replace(',', '').replace('%', '').replace('K', '').strip()
        all_data_list.append(data_text)

    pre_dict = {}
    pre_dict['date'] = all_data_list[0]
    pre_dict['cornClsprc'] = float(all_data_list[1])
    pre_dict['cornOpnprc'] = float(all_data_list[2])
    pre_dict['cornHgprc'] = float(all_data_list[3])
    pre_dict['cornLwprc'] = float(all_data_list[4])
    if all_data_list[5] != '':
        pre_dict['cornTrdvol'] = float(all_data_list[5].replace('.', '').strip()) * 10
    else:
        pre_dict['cornTrdvol'] = '0'
    pre_dict['cornTrdrate'] = float(all_data_list[6])

    return pre_dict


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
            tableName = 'tb_day_oksusu'
            columnList = ['date', 'cornClsprc', 'cornOpnprc', 'cornHgprc', 'cornLwprc', 'cornTrdvol', 'cornTrdrate']
            valsStr = (len(columnList) * '%s,')[:-1]
            row = get_one_data()
            cur.execute(f'''
            INSERT INTO {tableName} ({','.join(columnList)}, insert_dttm) 
            VALUES ({valsStr}, NOW())
            ON DUPLICATE KEY UPDATE
                cornClsprc = VALUES(cornClsprc),
                cornOpnprc = VALUES(cornOpnprc),
                cornHgprc = VALUES(cornHgprc),
                cornLwprc = VALUES(cornLwprc),
                cornTrdvol = VALUES(cornTrdvol),
                cornTrdrate = VALUES(cornTrdrate),
                insert_dttm = VALUES(insert_dttm);
            ''', (row['date'], row['cornClsprc'], row['cornOpnprc'], row['cornHgprc'], row['cornLwprc'], row['cornTrdvol'], row['cornTrdrate']))
            logger.info(row)
            logger.success(f'{tableName} DB insert success')
            conn.commit()
        return
    except Exception as ee:
        logger.error(f'DB insert Error: {ee}')


def main(main):
    sched = BackgroundScheduler(timezone='Asia/Seoul')
    # test용
    # sched.add_job(insert_mysql, 'cron', second='*/5', name='tb_day_oksusu', args=[config])
    # 실제 자동화 / 매일 오전 10시 20분 실행 예정 
    sched.add_job(insert_mysql, 'cron', hour='10', minute='20', name='tb_day_oksusu', args=[config])
    sched.start()

    while True:
        time.sleep(1)


if __name__ == '__main__':
    config = configparser.ConfigParser(os.environ)
    config.read('./config/config.cfg')

    # 로그 남기기
    logger.add(
        sink=config['log']['directory'] + '/tb_day_oksusu.log',
        enqueue=True,
        retention=int(config['log']['retention']),
        rotation=config['log']['rotation'],
        level=config['log']['level'].upper()
    )

    main(config)