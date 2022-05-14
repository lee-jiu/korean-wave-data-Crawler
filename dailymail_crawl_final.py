from requests.api import post
import uuid
from dateutil.parser import parse
# from tf.apps.crawler.task.dbconnect import db_con, get_keyword, insert_test_dailymail
import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.request import urlopen 
import re
from dbconnect import get_keyword, db_con, insert_news_search_post
from datetime import datetime, timedelta


def dailymail():
    model_kwargs = {
        'source_name': 'dailymail',
        'biz_kind': 'news',
        'country': 'UK',
        'methd': 'search',
        'category': 'article'
    }
    model_kwargs['view_count'] = None
    model_kwargs['vote'] = None
    model_kwargs['happy'] = None
    model_kwargs['unmoved'] = None
    model_kwargs['amused'] = None
    model_kwargs['excited'] = None
    model_kwargs['angry'] = None
    model_kwargs['sad'] = None
    model_kwargs['like_count'] = None
    model_kwargs['dislike_count'] = None


    # DB 접속
    conn = db_con()

    #keyword_list = get_keyword(conn=conn, source_name=model_kwargs['source_name'])
    keyword_list = [['공통', 'korean tv', 'COMMON'], ['공통', 'korean drama', 'COMMON'], ['공통', 'korean food', 'COMMON'],
                    ['공통', 'korean beauty', 'COMMON'], ['공통', 'BTS', 'COMMON'], ['공통', 'korean netflix', 'COMMON']]

    page_num = []

     
    for page__ in range(50, 2550, 50):
        page_num.append(page__)
     

    ignore = re.compile('[\n\r\t\xa0\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U0001F90D-\U0001F9FF\u202f]')

    for keyword_kr, key_, cat in keyword_list:
        model_kwargs['keyword'] = key_
        print(key_)
        print(keyword_kr)
        print(cat)
        for page_ in page_num:
            try:
                url = 'https://www.dailymail.co.uk/home/search.html?offset='+ str(page_) +'&size=50&sel=site&searchPhrase='+str(key_)+'&sort=relevant&type=article&type=video&type=permabox&days=last365days'
                      #'https://www.dailymail.co.uk/home/search.html?offset=0&size=50&sel=site&searchPhrase=korean+movie&sort=recent&type=article&type=video&type=permabox&days=all'
                #url = 'https://www.dailymail.co.uk/home/search.html?offset=0&size=50&sel=site&searchPhrase=dalgona&sort=recent&type=article&type=video&days=last365days'
                #url = 'https://www.dailymail.co.uk/home/search.html?offset=0&size=50&sel=site&searchPhrase=dalgona&sort=relevant&type=article&type=video&days=last365days'
                print("################"+url)
            except:
                break
            #검색 결과 페이지가 존재하지 않을 경우의 에러처리  
            result = requests.get(url)
            soup = BeautifulSoup(result.content, 'html.parser')

            # model_kwargs['post_date'] = soup.select('.sch-res-info')

            

            def get_text_without_children(tag):
                return ''.join(tag.find_all(text=True, recursive=False)).strip()

            content_list=soup.select('.sch-result.home')
            
            for content in content_list:
                postdate_data = content.select_one('.sch-res-info').text
                print(postdate_data)
                post_url = 'https://www.dailymail.co.uk' + content.find("a")["href"]
                if 'video' in post_url:
                    print('video continue')
                    continue
                print(post_url)
                post_date = parse(postdate_data.split(' -')[1].split(',')[0].strip())
                model_kwargs['post_date'] = post_date
                print(post_date)
                model_kwargs['tag'] = content.select_one('h2.sch-res-section').text.replace(' \n\xa0\n','')
                model_kwargs['title'] = content.select_one('.sch-res-title').text

                # post_url = 'https://www.dailymail.co.uk' + content.find("a")["href"]
                # # print(post_url)
                
                model_kwargs['url'] = post_url

                post_ = requests.get(post_url) 
                soup_ = BeautifulSoup(post_.content, 'html.parser')

                content_ = soup_.find("div",{"itemprop":"articleBody"})
                # print(content_)
                try:
                    content_data = str(content_.find_all("p"))
                    # print(content_data)
                    content_data = re.sub('<.+?>', '', content_data, 0).strip()
                    model_kwargs['content'] = ignore.sub(' ', content_data).strip()
                    model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')
                    
                    
                # 동영상 뉴스의 경우 본문의 내용이 없기 때문에 긁어오지 못해 오류 발생. 이를 위한 에러처리
                except:
                    model_kwargs['content'] = None
                    model_kwargs['post_date'] = None
                
                model_kwargs['keyword_kr'] = keyword_kr
                model_kwargs['comment_count'] = None

                post_date = post_date.date()
                print(post_date)
                first_date = '2021/11/01'
                first_date = datetime.strptime(first_date, '%Y/%m/%d')
                first_date = first_date.date()
                if post_date < first_date:  # 2021년 이전 게시물의 경우 건너뜀
                    print('skip before 2021')
                    continue

                second_date = '2021/11/30'
                second_date = datetime.strptime(second_date, '%Y/%m/%d')
                second_date = second_date.date()
                if post_date > second_date:  # 2021년 이전 게시물의 경우 건너뜀
                    print('skip after 2021')
                    continue

                print(model_kwargs)

                insert_news_search_post(conn=conn, model_kwargs=model_kwargs)           

            # content_data = content.extend(soup_.select('#js-article-text > div:nth-child(7) > p'))
            # time.sleep(0.3)

dailymail()