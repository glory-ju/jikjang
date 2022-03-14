import requests
from bs4 import BeautifulSoup
import re, os
import time, datetime
from time import sleep
from multiprocessing import Process
from loguru import logger
import urllib.parse
import db_conn
import configparser
import platform
from apscheduler.schedulers.background import BackgroundScheduler
from tendo import singleton
import pymysql


class NaverNewsCrawler:
    def __init__(self):
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
        self.duplicate_count = 5
        self.user_operating_system = str(platform.system())

    # [경제] - [금융] 카테고리의 당일 총 페이지 수 추출
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

    # 각 페이지마다의 20개 뉴스 기사들 url 리스트 추출
    def find_news_article_url(self, page_url):
        url_list = []
        page_response = requests.get(page_url, headers=self.headers)
        soup = BeautifulSoup(page_response.content, 'lxml')
        news_list_select = soup.select('body > div > table > tr > td > div > div > ul > li > dl > dt:nth-child(1) > a')

        logger.info(f'20개')
        for i in news_list_select:
            url = i['href']
            url = url.replace('amp;', '')
            url_list.append(url)
        return url_list

    # 마리아디비 연결
    @staticmethod
    def conn_mariadb(config_t):
        hostname = config_t['mariaDB']['hostname']
        port = config_t['mariaDB']['port']
        # username = urllib.parse.quote_plus(config_t['mariaDB']['username'])
        username = config_t['mariaDB']['username']
        # password = urllib.parse.quote_plus(config_t['mariaDB']['password'])
        password = config_t['mariaDB']['password']
        db_name = config_t['mariaDB']['db_name']
        charset = config_t['mariaDB']['charset']

        conn = pymysql.connect(host=hostname, port=int(port), user=username, password=password, database=db_name, charset=charset)
        logger.success('maria DB Connection Success')
        return conn

    def article_element(self, id, url_list):
        resp_1 = requests.get(url_list[id], headers=self.headers)
        soup_1 = BeautifulSoup(resp_1.content, 'lxml')

        sid1 = re.findall(r'\d+', url_list[id])[2]
        sid2 = re.findall(r'\d+', url_list[id])[4]
        oid = re.findall(r'\d+', url_list[id])[5]
        aid = re.findall(r'\d+', url_list[id])[6]

        try:
            time = soup_1.find('span', attrs={'class': 't11'}).get_text()
        except:
            time = ''
        try:
            media = soup_1.find('img')['title']
        except:
            media = soup_1.find('img')['alt']
        if soup_1.find('p', attrs={'class': 'b_text'}) == None:
            writer = ''
        elif soup_1.find('p', attrs={'class': 'b_text'}):
            writer = soup_1.find('p', attrs={'class': 'b_text'}).get_text().strip()
        try:
            title = soup_1.find('h3', attrs={'id': 'articleTitle'}).get_text()
        except:
            title = soup_1.find('h4', attrs={'class': 'title'}).get_text()

        try:
            raw_content = soup_1.find('div', attrs={'id': 'articleBodyContents'})
            if raw_content.select_one('em'):
                raw_content.select_one('em').decompose()
            content = ''.join(
                raw_content.text.replace('    ', '').replace('\n', '').replace('\t', '').replace('\'', '').strip())
        except:
            content = ''
        # try:
        #     content = soup_1.find('div', attrs={'id': 'articleBody'})
        # except:
        #     content = ''

        # elements = (date, sid1, sid2, oid, aid, time, media, writer, title, content, url_list[id])
        # news_info_list.append(elements)

        key = str(sid1) + str(sid2) + str(oid) + str(aid)
        return [key, title, media, writer, content, time]

    def make_query(self):
        pass

    def crawler(self, days, config_t):
        news_info_list = []

        pid = os.getpid()

        try:
            sql = '''
            CREATE TABLE IF NOT EXISTS logan_test(
                objectID varchar(24) NOT NULL PRIMARY KEY,
                date varchar(8),
                headline varchar(1024),
                category varchar(100),
                press varchar(100),
                writer varchar(100),
                content text,
                url varchar(512),
                news_time varchar(100))
            '''
            maria = self.conn_mariadb(config_t)
            cur = maria.cursor()
            cur.execute(sql)
        except Exception as eee:
            logger.critical(f'[{pid}] DB exception::{eee}')
            logger.critical(f'[{pid}] Crawling STOP ! ! !')
            exit()

        logger.debug(f'[{pid}] DB connected')

        cate = self.categories['경제']

        # 경제 카테고리의 세부 카테고리 구하기
        # url = f'https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1={cate}&sid2={subcate}&date={int(date)}'
        find_subcate_url = f'https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1={cate}'
        find_subcate_res = requests.get(find_subcate_url, headers=self.headers)
        find_subcate_soup = BeautifulSoup(find_subcate_res.content, 'lxml')
        # 세부 카테고리 find all
        subcate = find_subcate_soup.find('ul', attrs={'class': 'nav'}).find_all('li')
        subcategory_key_list = []
        for sub in subcate:
            subcategory_key_list.append(sub.text.strip())

        subcategory_value_list = []
        for i in subcategory_key_list:
            value = self.subcategories[i]
            subcategory_value_list.append(value)

        date = datetime.datetime.now()
        yesterday = date - datetime.timedelta(days=days)
        date = yesterday.strftime('%Y%m%d')


        for subcategory, subcategory_name in zip(subcategory_value_list, subcategory_key_list):
            logger.exception(f'[{pid}][{subcategory_name}] Crawling Start . . .')
            url = find_subcate_url + '&sid2=' + str(subcategory) + '&date=' + str(date)

            # totalpage는 네이버 페이지 구조를 이용해서 page=10000으로 지정해 totalpage를 알아냄
            # page=10000을 입력할 경우 페이지가 존재하지 않기 때문에 page=totalpage로 이동 됨 (Redirect)
            totalpage = self.find_news_totalpage(url + '&page=10000')
            made_urls = [url + '&page=' + str(page) for page in range(1, totalpage + 1)]

            for page_url in made_urls:
                print(page_url)
                url_list = self.find_news_article_url(page_url)

                for id in range(len(url_list)):
                    elements = self.article_element(id, url_list)
                    key = elements[0]
                    title = elements[1]
                    media = elements[2]
                    writer = elements[3]
                    content = elements[4]
                    time = elements[5]

                    # maria
                    if content != '' and key != '':
                        try:
                            query = f"""INSERT INTO logan_test (objectID, date, headline, category, press, writer, content, url, news_time)
                                    VALUES ('{key}', '{date}', '{title}', '경제', '{media}', '{writer}', '{content}', '{url_list[id]}', '{time}')"""
                            logger.info(f'[{pid}][경제][{subcategory_name}] DB insert::{title}')
                            cur.execute(query)

                            duplicate = 0
                        except Exception as eee:
                            duplicate += 1
                            logger.warning(f'[{pid}][경제][{subcategory_name}] DB exception::{eee}')

                            if duplicate >= self.duplicate_count:
                                break
                        maria.commit()
                    elif content == '':
                        logger.exception('NOT INCLUDED CONTENTS !!!')
                        continue
                if duplicate >= self.duplicate_count:
                    break
                if page_url.split('=')[-1] == str(totalpage): break

        logger.info(f'[{pid}][{cate}][{subcategory}] NewsCrawling Ended')
        maria.close()
        logger.complete()

    def start(self, config_t):
        # multiprocess 크롤링 시작
        logger.info('Naver News Crawling . . .start')
        logger.info(f'OS Type: {self.user_operating_system}')

        workers1, workers2 = [], []
        proc1 = Process(target=self.crawler, args=(0, config_t))
        proc2 = Process(target=self.crawler, args=(1, config_t))
        workers1.append(proc1)
        workers2.append(proc2)
        proc1.start()
        proc2.start()

        for w1, w2 in zip(workers1, workers2):
            w1.join()
            w2.join()

        logger.info('BYE !!!!')


if __name__ == '__main__':

    me = singleton.SingleInstance()
    days = 0
    start_time = time.time()

    scheduler = BackgroundScheduler()
    Crawler = NaverNewsCrawler()

    config = configparser.ConfigParser()
    config.read('C:/Users/logan/PycharmProjects/jikjang/logan/config.cfg', encoding='UTF8')

    logger.add(config.get('log', 'filename'), level=config.get('log', 'level'),
               rotation=config.get('log', 'rotation'), retention=int(config.get('log','retention')), enqueue=True,
               encoding='utf8')

    scheduler.add_job(func=Crawler.start, trigger='interval', seconds=int(config.get('DEFAULT', 'interval_time')),
                      args=[config])
    scheduler.start()

    while True: sleep(1)