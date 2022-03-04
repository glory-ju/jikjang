import requests
from bs4 import BeautifulSoup
# from practice.crawling import crawler
# from practice.cate_crawling import news_20_crawling
import re
import pandas as pd
import datetime

days = 0
page = 1
data_frame = []

while days <= 10:
    date = datetime.datetime.now()
    yesterday = date - datetime.timedelta(days=days)
    date = yesterday.strftime('%Y%m%d')
    # print(date)
    url = f'https://news.naver.com/main/list.naver?mode=LS2D&mid=shm&sid1=101&sid2=259&date={int(date)}&page={page}'

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    }

    resp = requests.get(url, headers=headers)
    print(resp)
    soup = BeautifulSoup(resp.content, 'lxml')

    news_list_select = soup.select('body > div > table > tr > td > div > div > ul > li > dl > dt:nth-child(1) > a')
    url_list = []
    for i in news_list_select:
        url = i['href']
        url = url.replace('amp;', '')
        url_list.append(url)
    print(f'page: {page}', url_list)

    for idx in range(len(url_list)):
        resp_1 = requests.get(url_list[idx], headers=headers)
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

        elements = (date, sid1, sid2, oid, aid, time, media, writer, title, content, url_list[idx])
        print(elements)
        # if elements not in data_frame:
        if len(data_frame) == len(set(data_frame)):
            data_frame.append(elements)
        else:
            days += 1
            page = 0
            data_frame.pop()
            break
    df = pd.DataFrame(data_frame, columns=['today', 'sid1', 'sid2', 'oid', 'aid', 'time', 'media', 'writer', 'title', 'content', 'url'])
    df.to_csv('test1.csv', index=False, encoding='utf-8')
    page += 1
