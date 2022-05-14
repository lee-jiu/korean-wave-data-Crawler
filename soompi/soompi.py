from requests.api import post, request
import uuid 
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import requests
import json
import urllib
import re
from datetime import datetime , date
from dbconnect import db_con, insert_sns_post
from soompi_comment import soompi_comment

# 실행 방법 : (visual studio code 기준) 터미널 창에 
# python > from soompi import soompi > soompi() 입력 
# soompi_comment.py는 실행할 필요 없음. 본문+댓글 같이 코드 연결되어 수집됨  



def soompi():
   
    model_kwargs = {
       'biz_kind' : 'post',
       'category' : None,
       'source_name' : 'soompi',
       'keyword' : None,
       'keyword_kr' : None,
       'page_name' : None,
       'share_count' : None,
       'comment_count' : None,
       'like_count' : None,
       'retweet_count' : None,
       'typ' : 'text',
       'dislike_count' : None,
       'view_count' : None,
       'vote' : None,
       'methd' : 'page'
    }
   
    conn = db_con()
    
    #리스트에 페이지 번호 순차 삽입 
    page_num = [1]  
    page_n = 1
    
            
    #불필요한 특수문자 제거
    ignore = re.compile('[\n\r\t\xa0\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U0001F90D-\U0001F9FF\u202f]')


    for page_ in page_num:
        try:
            
            #각 페이지 url 
            url = 'https://api-fandom.soompi.com/posts.json?sort=latest&page='+ str(page_) + '&perPage=15'
            
            text_data = urllib.request.urlopen(url).read().decode('utf-8') 
            soompi = json.loads(text_data)
        except:
            break
   
        for i in soompi['results']:
            model_kwargs['page_id'] = i['id']
            model_kwargs['url'] = i['url']['web']
            # date = datetime.datetime.fromtimestamp(int(i['createdAt'])).strftime('%m/%d') 
            date_ = datetime.fromtimestamp(i['createdAt']).date()
        #   if date_== date(2021, 10, 31):
            
            #스킵해야할 날짜 구간 설정
            if date_ > date(2021, 11, 30) :
                continue
            if date_ < date(2021, 11, 1) :
                continue

            # 오류나는 게시물의 경우 페이지 id 삽입 후 스킵 
            if i['id'] == '1485593wpp'  :
                continue
            #(해당 게시물의 경우 내용이 너무 길어서 적재가 안되는 것으로 추정) 


            model_kwargs['post_date'] = date_
            model_kwargs['title'] = i['title']['text']
            model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')


 

            post_url = i['url']['web']
            # 각 게시물의 url 가져오기 
            result = requests.get(post_url)
            soup = BeautifulSoup(result.content, 'html.parser')
            
            #본문 내용 가져오기 
            content_data = str(soup.select('#app > div.article-body.container-main > div.container.article-content.pos-relative > div.row > div.col.small-12.med-8.article-section > div > div.article-wrapper > div:nth-child(1)'))       
            content_data = re.sub('<.+?>', '', content_data, 0).strip()
            content_data = ignore.sub(' ', content_data).strip()
            model_kwargs['content'] = content_data

            print(model_kwargs)
            insert_sns_post(conn=conn, model_kwargs=model_kwargs) 

            #각 게시물마다 달린 댓글 가져오기 
            soompi_comment(conn, model_kwargs['data_id'], model_kwargs['page_id'], model_kwargs['title'], model_kwargs['url'], model_kwargs['post_date'])
       
        page_n = page_n + 1 
        page_num.append(page_n)
        print(page_num)

    conn.close()