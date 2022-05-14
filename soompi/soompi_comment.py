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
from dbconnect import db_con, insert_sns_comment


# 실행방법 : soompi.py 파일을 실행할 경우 연동되어 실행됨. 단독으로 실행 불가능

def soompi_comment(conn, data_id, art_id, art_title, art_url, post_date):
   
    model_kwargs = {
       'source_name' : 'soompi',
       'post_date': post_date
    }
   
    # conn = db_con()
    '''
    page_num = [1]  
    page_n = 1
    
            
    for page_ in page_num:
        try:
            url = 'https://api-fandom.soompi.com/posts.json?sort=latest&page='+ str(page_) + '&perPage=15'
            text_data = urllib.request.urlopen(url).read().decode('utf-8') 
            soompi = json.loads(text_data)
        
        except:
            break
        
        for i in soompi['results']:
            model_kwargs['url'] = i['url']['web']
            date_ = datetime.fromtimestamp(i['createdAt']).date()
            if date_ < date(2021, 1, 1) :
                quit()
            model_kwargs['post_date'] = date_    
        
            art_id = i['id'] 
            art_title = i['title']['text'] 
            art_url = i['url']['web']
            '''
    #model_kwargs['post_date'] = post_date
    
    #댓글 정보 페이지 url 가져오기 
    comment_url = 'https://disqus.com/embed/comments/?base=default&f=soompi&t_i=' + str(art_id) + '&t_u=' + str(art_url) + '&t_e=' + str(art_title) + '&t_t='+ str(art_title) + '&s_o=desc&l=en_US'
    

    result_ = requests.get(comment_url)
    soup_ = BeautifulSoup(result_.content, 'html.parser')

    com_data = soup_.select("#disqus-threadData")
    com_txt = str(com_data)
    #불필요한 태그 제거 
    com_parse = com_txt.replace('[<script id="disqus-threadData" type="text/json">','').replace('</script>]','').replace('</p>', '').replace('<p>','')
    com_txt_ = json.loads(com_parse)

    n = 1

    #각 댓글들 요소 추출 후 DB 적재 
    for i in (com_txt_['response']['posts']) :
        model_kwargs['url'] = art_url + '#' + str(n) 
        n = n+1
        model_kwargs['like_count'] = i['likes']
        model_kwargs['content'] = str(i['message']).replace('<p>','').replace('</p>','')
        model_kwargs['writer'] =  i['author']['name']
        model_kwargs['data_id'] = data_id
        model_kwargs['comment_id'] = str(uuid.uuid4()).replace('-','')



        print(model_kwargs)
        insert_sns_comment(conn=conn, model_kwargs=model_kwargs) 
        '''    
        page_n = page_n + 1 
        page_num.append(page_n)
        print(page_num)
        '''