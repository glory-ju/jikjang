# -*- coding: utf-8 -*-
import requests
import re, os, configparser, platform, pymysql
import pandas as pd
import urllib.parse
import argparse
import zmq
from bs4 import BeautifulSoup
from time import sleep
from loguru import logger
from multiprocessing import Process
from datetime import datetime, timedelta

from exceptions import *
import db_mongo

#cron
from apscheduler.schedulers.background import BackgroundScheduler
# 중복 실행 방지
from tendo import singleton



class NaverNewsCrawler():
    def __init__(self, config):
        self.config = config
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36', }
        self.categories = {'정치': 100, '경제': 101, '사회': 102, '생활/문화': 103, 'IT/과학': 105, '세계': 104}
        self.subcategories = {'청와대': 264, '국회/정당': 265, '북한': 268, '행정': 266, '국방/외교': 267, '정치일반': 269, '금융': 259,
                              '증권': 258, '산업/재계': 261, '중기/벤처': 771, '부동산': 260, '글로벌 경제': 262, '생활경제': 310,
                              '경제 일반': 263,
                              '사건사고': 249, '교육': 250, '노동': 251, '언론': 254, '환경': 252, '인권/복지': '59b', '식품/의료': 255,
                              '지역': 256, '인물': 276, '사회 일반': 257,
                              '건강정보': 241, '자동차/시승기': 239, '도로/교통': 240, '여행/레저': 237, '음식/맛집': 238, '패션/뷰티': 376,
                              '공연/전시': 242, '책': 243, '종교': 244, '날씨': 248, '생활문화 일반': 245,
                              '모바일': 731, '인터넷/SNS': 226, '통신/뉴미디어': 227, 'IT 일반': 230, '보안/해킹': 732, '컴퓨터': 283,
                              '게임/리뷰': 229, '과학 일반': 228,
                              '아시아/호주': 231, '미국/중남미': 232, '유럽': 233, '중동/아프리카': 234, '세계 일반': 322}

        self.selected_categories = []
        self.selected_subcategories = []

        self.date = {'start':20220316, 'end': 20220316}
        self.user_operating_system = str(platform.system())
        self.duplicate_count = 5

        # self.context_push = zmq.Context()
        # self.z_socket_push = self.context_push.socket(zmq.PUSH)
        # self.z_socket_push.connect("tcp://localhost:5555")
        #
        # self.context_pull = zmq.Context()
        # self.z_socket_pull = self.context_pull.socket(zmq.PULL)
        # self.z_socket_pull.bind("tcp://localhost:5556")

    def set_category(self, *args):
        if args[0] == '':
            self.selected_categories = ['경제', '정치', '사회', '생활/문화', '세계', 'IT/과학']
        else:
            temp = []
            for key in list(args):
                if self.categories.get(key) is None:
                    raise InvalidCategory(key)
                else:
                    temp.append(key)
            self.selected_categories = temp

    def set_subcategory(self, *args):
        if args[0] == '':
            self.selected_subcategories = None
        else:
            temp = []
            for key in args:
                if self.subcategories.get(key) is not None:
                    temp.append(key)
            self.selected_subcategories = temp

    def set_date_range(self, start_day, end_day):
        if start_day > end_day:
            logger.critical('Wrong Date !!!!!!!')
            exit()
        args = [start_day, end_day]
        for key, date in zip(self.date, args):
            self.date[key] = date

    def find_subcategory(self, url):
        # 서브카테고리를 카테고리로부터 알아냄
        res = requests.get(url, headers=self.headers)
        doc = BeautifulSoup(res.content, 'lxml')
        subcategory_list = doc.select('#snb > ul > li')
        p = re.compile(r'sid2=(\d+)')
        check_hangeul = re.compile('[가-힣]+')
        links = []
        names = []

        for line in subcategory_list:
            link = line.a.get('href')
            name = line.get_text().strip()
            try:
                lnk = p.findall(link)[0]
                links.append(lnk)
                names.append(name)
            except: pass
        del subcategory_list

        return links, names

    @staticmethod
    def make_news_page_url(category_url, start_day, end_day):
        made_urls_days = []
        start_day, end_day = str(start_day), str(end_day)
        dt_index = pd.date_range(start=start_day, end=end_day)
        dt_list = dt_index.strftime('%Y%m%d').tolist()

        for i in list(reversed(dt_list)):
            year = i[0:4]
            month = i[4:6]
            month_day = i[-2:]

            url = category_url + str(year) + str(month) + str(month_day)
            made_urls_days.append(url)
        return made_urls_days

    # 서브카테고리별 총 페이지 수 구하기
    def find_news_totalpage(self, url):
        try:
            totalpage_url = url
            request_content = requests.get(totalpage_url, headers=self.headers)
            document_content = BeautifulSoup(request_content.content, 'lxml')
            headline_tag = document_content.find('div', {'class': 'paging'}).find('strong')
            regex = re.compile(r'<strong>(?P<num>\d+)')
            match = regex.findall(str(headline_tag))
            return int(match[0])
        except Exception:
            return 0

    def get_url_data(self, url, max_tries=10):
        remaining_tries = int(max_tries)

        while remaining_tries > 0:
            try:
                return requests.get(url, headers=self.headers)
            except requests.exceptions:
                sleep(60)
            remaining_tries -= 1
        raise ResponseTimeout()

    def conn_mongodb(self):
        hostname = self.config['mongoDB']['hostname']
        port = self.config['mongoDB']['port']
        username = urllib.parse.quote_plus(self.config['mongoDB']['username'])
        password = urllib.parse.quote_plus(self.config['mongoDB']['password'])
        db_name = self.config['mongoDB']['db_name']

        return db_mongo.set_mongodb(hostname, port, username, password, db_name)

    def conn_mariadb(self):
        hostname = self.config['mariaDB']['hostname']
        port = self.config['mariaDB']['port']
        username = urllib.parse.quote_plus(self.config['mariaDB']['username'])
        password = urllib.parse.quote_plus(self.config['mariaDB']['password'])
        database = self.config['mariaDB']['database']
        charset = self.config['mariaDB']['charset']

        conn = pymysql.connect(host=hostname, port=int(port), user=username, password='nsroot1234)(*&', database=database, charset=charset)
        logger.info('maria DB connection SUCCESS!!!')

        return conn

    def crawling(self, category_name, subcategory):
        pid = os.getpid()
        mongo = None

        logger.info(f'[{pid}][{category_name}] Crawling Start . . . . .')

        try:
            mongo = self.conn_mongodb()
        except Exception as eee:
            logger.critical(f'[{pid}][{category_name}] DB Exception::{eee}')
            logger.critical(f'[{pid}][{category_name}] Crawling STOP !!!!!!')
            exit()
        logger.debug(f'[{pid}][{category_name}] DB CONNECTED')

        urls_category = [] # 서브 카테고리별 url
        subcategory_names = [] # 서브 카테고리 이름들

        # 기사 URL 서브카테고리 유무에 따라서 없으면 서브카테고리 구함.
        if subcategory is None:
            url_temp = f'https://news.naver.com/main/list.nhn?mode=LSD&mid=shm&sid1={str(self.categories.get(category_name))}&date='
            sid2es, subcate_names = self.find_subcategory(url_temp)

            for sid2, subcate_name in zip(sid2es, subcate_names):
                base_url = f'https://news.naver.com/main/list.nhn?mode=LSD&mid=shm&sid1={str(self.categories.get(category_name))}&sid2={str(sid2)}&date='

                urls_category.append(base_url)
                subcategory_names.append(subcate_name)
            del sid2es, subcate_names

        else:
            # 서브카테고리가 지정되면 하나의 서브카테고리에 프로세스 하나씩 할당됨
            sid2 = self.subcategories.get(subcategory)
            url = f'https://news.naver.com/main/list.nhn?mode=LSD&mid=shm&sid1={str(self.categories.get(category_name))}&sid2={str(sid2)}&date='
            urls_category.append(url)
            subcategory_names.append(subcategory)

        for url, sub in zip(urls_category, subcategory_names):
            day_urls = self.make_news_page_url(url, self.date['start'], self.date['end'])
            logger.info(f'[{pid}][{category_name}][{sub}] 크롤링 시작...')

            duplicate = 0
            # print(day_urls)
            # sub 카테고리 리스트 > 날짜별 리스트
            for i in day_urls:
                regex = re.compile(r'date=(\d+)')
                news_date = regex.findall(i)[0]

                # totalpage는 네이버 페이지 구조를 이용해서 page=10000으로 지정해 totalpage를 알아냄
                # page=10000을 입력할 경우 페이지가 존재하지 않기 때문에 page=totalpage로 이동 됨 (Redirect)
                totalpage = self.find_news_totalpage(i + '&page=10000')
                made_urls = [i + '&page=' + str(page) for page in range(1, totalpage + 1)]

                # sub 카테고리 리스트 > 날짜별 리스트 > 뉴스 리스트
                for URL in made_urls:
                    request = self.get_url_data(URL)
                    document = BeautifulSoup(request.content, 'lxml')

                    # html - newsflash_body - type06_headline, type06
                    # 각 페이지에 있는 기사들 가져오기
                    post_temp = document.select('.newsflash_body .type06_headline li dl')
                    post_temp.extend(document.select('.newsflash_body .type06 li dl'))

                    # 각 페이지에 있는 기사들의 url 저장
                    post = [line.a.get('href') for line in post_temp]
                    del post_temp

                    for content_url in post:
                        request_content = self.get_url_data(content_url)
                        document_content = BeautifulSoup(request_content.content, 'lxml')

                        # 기사 업로드 시간 / 최종 수정 시간
                        try:
                            article_time = document_content.find_all('span', {'class': 't11'})
                            str_article_date_edit = (str(article_time[1].find(text=True)))
                        except Exception as eee:
                            str_article_date_edit = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')
                        try:
                            article_time = document_content.find_all('span', {'class': 't11'})
                            str_article_date_input = (str(article_time[0].find(text=True)))
                            art_url = content_url
                            ne_date = news_date
                        except Exception as eee:
                            str_article_date_input = ''
                            ne_date = news_date
                            gisa_urls = document_content.find_all('meta', {'property': 'og:url'})
                            gisa_url = str(gisa_urls[0].get('content'))
                            art_url = gisa_url
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        # 기자
                        try:
                            writer = document_content.find_all('p', {'class': 'b_text'})
                            str_writer = str(writer[0].find(text=True).strip())
                        except Exception as eee:
                            str_writer = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        # 기사 제목
                        try:
                            headline = document_content.find_all('h3', {'id': 'articleTitle'}, {'class': 'tts_head'})
                            str_headline = str(headline[0].find(text=True))
                        except Exception as eee:
                            str_headline = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        # 기사 본문
                        try:
                            raw_content = document_content.select('#articleBody')
                            if raw_content[0].select_one('em'):
                                raw_content[0].select_one('em').decompose()
                            str_content = str(raw_content[0])
                        except Exception as eee:
                            str_content = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        # 기사 언론사
                        try:
                            press = document_content.find('meta', {'property': 'me2:category1'})
                            str_press = str(press['content'])
                        except Exception as eee:
                            str_press = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        regex_sid1 = re.compile(r'sid1=(\d+)')
                        regex_sid2 = re.compile(r'sid2=(\d+)')
                        regex_oid = re.compile(r'oid=(\d+)')
                        regex_aid = re.compile(r'aid=(\d+)')

                        try:
                            sid1 = regex_sid1.findall(art_url)[0]
                            sid2 = regex_sid2.findall(art_url)[0]
                            oid = regex_oid.findall(art_url)[0]
                            aid = regex_aid.findall(art_url)[0]
                        except Exception as eee:
                            logger.warning(f'Invalid Redirect URL: {url} --> {art_url}')
                            sid1 = ''
                            sid2 = ''
                            oid = ''
                            aid = ''
                            logger.debug(f'[{pid}][{category_name}][{sub}] {eee}')

                        key = str(oid) + str(aid)

                        # DB insert
                        if str_content != '' and key != '':
                            try:
                                db_mongo.mongo_insert({'_id':key, 'sid1':sid1, 'sid2':sid2, 'oid':oid, 'aid':aid,
                                              'news_date': ne_date, 'category': category_name,
                                              'subcategory': sub, 'press':str_press, 'writer':str_writer, 'headline':str_headline,
                                              'body_raw':str_content, 'article_date':str_article_date_input,
                                              'article_editdate':str_article_date_edit, 'content_url':art_url,
                                              'insertTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')},'test_data', mongo)
                                logger.info(f'[{pid}][{category_name}][{sub}] DB insert::{str_headline}')

                                duplicate = 0
                            except Exception as eee:
                                duplicate += 1
                                logger.warning(f'[{pid}][{category_name}][{sub}] DB exception::{eee}')

                                if duplicate >= self.duplicate_count:
                                    break

                        elif str_content == '':
                            continue

                    if duplicate >= self.duplicate_count:
                        break

                if duplicate >= self.duplicate_count:
                    # 10개 이상 중복이면 이미 크롤링된 뉴스라고 판단
                    logger.info(f'[{pid}][{category_name}][{sub}] NewsCrawling duplicate Ended => {duplicate}')
                    break
        logger.info(f'[{pid}][{category_name}] NewsCrawling Ended')
        logger.complete()

    def start(self):
        # MultiProcessing
        logger.info('Naver News Crawling ... start')
        logger.info(f'OS Type: {self.user_operating_system}')

        today = datetime.now().strftime('%Y%m%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        self.set_date_range(yesterday, today)
        self.duplicate_count = int(self.config['DEFAULT']['duplicate_count'])

        logger.info(self.date)

        workers = []
        if self.selected_subcategories is not None:
            for subcategory_name in self.selected_subcategories:
                proc = Process(target=self.crawling, args=(self.selected_categories[0], subcategory_name))
                workers.append(proc)
                proc.start()

            for w in workers:
                w.join()

            logger.info('BYE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        else:
            for category_name in self.selected_categories:
                proc2 = Process(target=self.crawling, args=(category_name, None))
                workers.append(proc2)
                proc2.start()

            for w2 in workers:
                w2.join()

            logger.info('BYE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

def content_preprocessing(str_content):
    special_symbol = re.compile('[\{\}\/?,;:|*~`!^\-_+<>@\#$&▲ㅣ▶◆◇◀■【】\\\=\'\"]')
    special_symbol_2 = re.compile('[\{\}\/\[\]?,;:|\)*~`!^\-_+<>@\#$&▲ㅣ▶◆◇◀■【】\\\=\(\'\"]')
    content_pattern = re.compile('본문 내용|TV플레이어| 동영상 뉴스|flash 오류를 우회하기 위한 함수 추가function  flash removeCallback|tt|앵커 멘트|xa0|사진')
    context_pattern = re.compile(r'앵커멘트|.사진=\S+.|.\S+제공.|촬영\S\S\S\S|\S\S\S 기자|\[(.*?)\]|\]|\((.*?)뉴스\)|\S{2,4}뉴스')
    bot_text = re.compile('기사 자동생성 알고리즘|로보뉴스|로봇뉴스|로보 뉴스|로봇 뉴스|로봇 기자')

    text1 = str_content.replace('\\n', '').replace('\\t', '').replace('\\r', '')
    text2 = re.sub(special_symbol, ' ', text1)
    text3 = re.sub(content_pattern, ' ', text2)
    text4 = re.sub(context_pattern, ' ', text3)
    text5 = re.sub(' +', ' ', text4).lstrip()
    reversed_content = ''.join(reversed(text5))
    content = ''
    for i in range(len(text5)):
        # reverse된 기사 내용 중, '.다'로 끝나는 경우 기사 내용이 끝난 것이기 때문에 기사 내용이 끝난 후의 광고, 기자 등의 정보는 다 지움
        if (reversed_content[i:i+2] == '.다') or (reversed_content[i:i+2] =='.시공'):
            content = ''.join(reversed(reversed_content[i:]))
            break

    if bot_text.findall(content) or (len(content) <= 40): # 자동생성 글자가 들어가면 본문 아예 비움 / 짧은 기사 거름
        return None
    else: return content

def main_preprocessig_content(config):
    CNN = NaverNewsCrawler(config)
    conn = CNN.conn_mongodb()['test_data']

    # mongodb에서 100개 데이터가 생성되면 가지고 오기. zmq로
    datas = conn.find()
    content_list = []
    # for data in datas:
    #     sid1 = data['sid1']
    #     sid2 = data['sid2']
    #     oid = data['oid']
    #     aid = data['aid']
    #     id_list.append([sid1, sid2, oid, aid])
    #     print([sid1, sid2, oid, aid])
    #
    # headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36', }
    # for id in id_list:
    #     url = f'https://news.naver.com/main/read.naver?mode=LS2D&mid=shm&sid1={id[0]}&sid2={id[1]}&oid={id[2]}&aid={id[3]}'
    #     req = requests.get(url, headers=headers)
    #     soup = BeautifulSoup(req.content)
    for i, data in enumerate(datas):
        body_raw = data['body_raw']
        soup = BeautifulSoup(body_raw, 'lxml')
        contents = soup.find('div', {'id':'articleBodyContents'})
        content = contents.text.strip()
        pre_content = content_preprocessing(content)
        content_list.append(pre_content)
    logger.info('mongoDB raw contents -> preprocessing content SUCCESS!!!')
    return datas, content_list

        # for content in contents.children:
        #     if type(content) == Comment:
        #         continue
        #     elif type(content) == NavigableString:
        #         pre_content += content
        #     elif type(content) == Tag:
        #     if i == 99 and type(content) == bs4.element.Tag:
        #         print(i, type(content), content)

def push_mariadb(config):
    query = '''
            CREATE TABLE IF NOT EXISTS logan_test(
            objectID varchar(24) not null primary key,
            date varchar(8),
            headline varchar(1024),
            category varchar(100),
            press varchar(100),
            content text,
            url varchar(512))
            '''
    CNN = NaverNewsCrawler(config)
    conn = CNN.conn_mariadb()
    cursor = conn.cursor()
    cursor.execute(query)
    logger.info('TABLE CREATION SUCCESS')

    datas, content = main_preprocessig_content(config)

    for i, data in enumerate(datas):
        print('왜 안 돼')
        objectID = data['_id']
        date = data['news_date']
        headline = data['headline']
        category = data['category']
        press = data['press']
        url = data['content_url']
        print(objectID, date, headline, category, press, url)
        print(content[i])
        insert_query = f"""INSERT INTO logan_test VALUES 
        ('{objectID}', '{date}', '{headline}', '{category}', '{press}', '{content[i]}', '{url}')
        """
        cursor.execute(insert_query)
    logger.info('mongo DB -> preprocessing content -> maria DB // PUSH SUCCESS!!')
    conn.close()
    logger.complete()

if __name__ == '__main__':
    me = singleton.SingleInstance()

    parser = argparse.ArgumentParser(description='DB STORE via ZMQ')
    parser.add_argument('-c', '--cfg', nargs='?', required=True, default='./news.cfg', metavar='CFG',
                        help='set config file')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.cfg, encoding='UTF8')
    config = None
    try:
        config = configparser.ConfigParser()
        config.read(args.cfg, encoding='UTF8')
    except Exception as e:
        exit()

    # if args.end:
    #     logger.add(config.get('log', 'filename_zmq'), level=config.get('log', 'level'), rotation=config.get('log', 'rotation'),
    #                           retention=int(config.get('log', 'retention')), enqueue=True, encofig='UTF8')
    #     main(config, args.start, args.end)

    logger.add(config['log']['filename'], level=config['log']['level'], rotation=config['log']['rotation'],
                          retention=int(config['log']['retention']), enqueue=True, encoding='UTF8')

    crawler = NaverNewsCrawler(config)
    crawler.set_category(config['DEFAULT']['Main_Category'])
    crawler.set_subcategory(config['DEFAULT']['Sub_Category'])
    # crawler.start()
    # main_preprocessig_content(config)
    push_mariadb(config)
'''
    scheduler = BackgroundScheduler()
    crawler = NaverNewsCrawler(config)

    crawler.set_category(config['DEFAULT']['Main_Category'])
    crawler.set_subcategory(config['DEFAULT']['Sub_Category'])

    scheduler.add_job(func=crawler.start, trigger='interval', seconds=int(config['DEFAULT']['interval_time']))

    scheduler.start()

    while True: sleep(1)
'''



