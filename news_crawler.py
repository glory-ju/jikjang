import requests
from bs4 import BeautifulSoup
import re, os
import pandas as pd
import datetime, time
import multiprocessing
from multiprocessing import Process

class NaverNewsCrawler():
    def __init__(self):
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',}

    # [경제] - [금융] 카테고리의 당일 총 페이지 수 추출
    def find_news_totalpage(self, url):
        try:
            totlapage_url = url
            request_content = requests.get(totlapage_url,headers=self.headers)
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

        for i in news_list_select:
            url = i['href']
            url = url.replace('amp;', '')
            url_list.append(url)
        return url_list

    # 뉴스 기사당 코드, 언론사, 기자, 제목, 내용 등의 데이터 추출
    def find_news_content_elements(self, idx, date, url_list):
        resp_1 = requests.get(url_list[idx], headers=self.headers)
        soup_1 = BeautifulSoup(resp_1.content, 'lxml')

        sid1 = re.findall(r'\d+', url_list[idx])[2]
        sid2 = re.findall(r'\d+', url_list[idx])[4]
        oid = re.findall(r'\d+', url_list[idx])[5]
        aid = re.findall(r'\d+', url_list[idx])[6]

        time = soup_1.find('span', attrs={'class': 't11'}).get_text()
        media = soup_1.find('img')['title']
        if soup_1.find('p', attrs={'class': 'b_text'}) == None:
            writer = ''
        elif soup_1.find('p', attrs={'class': 'b_text'}):
            writer = soup_1.find('p', attrs={'class': 'b_text'}).get_text().strip()
        title = soup_1.find('title').get_text()

        raw_content = soup_1.find('div', attrs={'id': 'articleBodyContents'})
        if raw_content.select_one('em'):
            raw_content.select_one('em').decompose()

        content = raw_content.text.replace('    ', '').strip()

        proc = os.getpid()
        print(f'Process ID : {proc}')
        elements = (date, sid1, sid2, oid, aid, time, media, writer, title, content, url_list[idx])
        print(elements)
        return elements

    def crawler(self, days):
        news_info_list = []
        while True:
            date = datetime.datetime.now()
            yesterday = date - datetime.timedelta(days=days)
            date = yesterday.strftime('%Y%m%d')

            url = f'https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1=101&sid2=259&date={int(date)}'

            resp = requests.get(url, headers=self.headers)
            print(resp)
            soup = BeautifulSoup(resp.content, 'lxml')

            # totalpage는 네이버 페이지 구조를 이용해서 page=10000으로 지정해 totalpage를 알아냄
            # page=10000을 입력할 경우 페이지가 존재하지 않기 때문에 page=totalpage로 이동 됨 (Redirect)
            totalpage = self.find_news_totalpage(url + '&page=10000')
            made_urls = [url + '&page=' + str(page) for page in range(1, totalpage + 1)]

            for page_url in made_urls:
                print(page_url)
                url_list = self.find_news_article_url(page_url)

                for idx in range(len(url_list)):
                    elements = self.find_news_content_elements(idx, date, url_list)
                    news_info_list.append(elements)
                df = pd.DataFrame(news_info_list,
                                  columns=['today', 'sid1', 'sid2', 'oid', 'aid', 'time', 'media', 'writer', 'title',
                                           'content', 'url'])
                df.to_csv('test2.csv', index=False, encoding='utf-8')

                if page_url.split('=')[-1] == str(totalpage): break
            break
if __name__ == '__main__':
    days = 0
    start_time = time.time()

    day_process = [0, 1, 2, 3]

# multi processing - Pool : 156sec
#     pool = multiprocessing.Pool(processes=4)
#     pool.map(NaverNewsCrawler().crawler, day_process)
#     pool.close()
#     pool.join()
#
#     print(time.time() - start_time)

# multi processing - Process : 166sec
#     procs = []
#
#     for index, number in enumerate(day_process):
#         proc = Process(target=NaverNewsCrawler().crawler, args=(number,))
#         procs.append(proc)
#         proc.start()
#
#     for proc in procs:
#         proc.join()
#
#     print(time.time() - start_time)

# single processing : 276sec
    for day in day_process:
        crawler = NaverNewsCrawler().crawler(day)
    print(time.time() - start_time)