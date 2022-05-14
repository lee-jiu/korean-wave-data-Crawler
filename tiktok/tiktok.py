from TikTokApi import TikTokApi
import pandas as pd
import json 
from collections import OrderedDict
from datetime import datetime
# from .dbconnect import db_con, insert_sns_post

# def tiktok_post():
   
#     model_kwargs = {
#        'biz_kind' : 'post',
#        'category' : None,
#        'source_name' : 'tiktok',
#        'keyword' : None,
#        'keyword_kr' : None,
#        'page_name' : None,
#        'title': None,
#        'love' : None,
#        'wow' : None,
#        'haha' : None,
#        'sad' : None,
#        'angry' : None,
#        'thankful' : None,
#        'retweet_count' : None,
#         'typ' : 'video',
#        'dislike_count' : None,
#        'view_count' : None,
#        'vote' : None,
#        'method' : 'page'
#     }

# def tiktok_page():
   
#     model_kwargs = {
#        'biz_kind' : 'page',
#        'content_count' : None,
#        'play_count' : None,
#        'popularCountry1' : None,
#        'popularCountry2' : None,
#        'popularCountry3' : None,
#        'view_count' : None,
#        'vote' : None,
#        'comment_count' : None
#     }
   
   
#     conn = db_con()

file_data = OrderedDict()

verifyFp = "verify_kjrpy376_43tD17bY_o9AA_4HFS_8hsr_h8RUEw4aajy7"

api = TikTokApi.get_instance(custom_verifyFp=verifyFp, use_test_endpoints=True)

cookie = {
  "s_v_web_id": "<O_Yjlp_4HOb_9btz_PMguaHPH05E9>",
  "tt_webid": "<6986512204028757506>"
}

count = 1000


username_list = ['aespa_official']

for user_n in username_list:
    try:
      tiktoks = api.by_username(str(user_n), count=count)
      # print(tiktoks)
      # tiktoks = api.byHashtag(str(key_), count=count, offset=offset_)

    except:
      continue


    for tiktok_post in tiktoks:
      print(tiktok_post['id'])
      print(tiktok_post['desc'])
      print(tiktok_post['video']['playAddr'])
      print(tiktok_post['stats']['diggCount'])
      print(tiktok_post['stats']['playCount'])
      print(tiktok_post['author']['id'])
      print(tiktok_post['author']['uniqueId'])
      print(datetime.fromtimestamp(int(tiktok_post['createTime'])).date()) 
      # model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')


    # for tiktok_page in tiktoks:  
    #   print(tiktok_page['author']['id'])
    #   print(tiktok_page['author']['uniqueId'])
    #   print(tiktok_page['author']['nickname'])
    #   print(tiktok_page['authorStats']['heart'])
    #   print(tiktok_page['authorStats']['followerCount'])
    #   print(tiktok_page['authorStats']['followingCount'])
    #   print(datetime.datetime.fromtimestamp(int(tiktok_page['createTime'])).date()) 
    #   print(tiktok_page['authorStats']['videoCount'])
   






# count = 50


# username_list = ['aespa_official']
# offset_num = [0, 1000000]

# for user_n in username_list:
#   for offset_ in offset_num:
#     try:
#       tiktoks = api.by_username(str(user_n), count=count, offset=offset_)
#       print(tiktoks)
#       # tiktoks = api.byHashtag(str(key_), count=count, offset=offset_)
#     except:
#       continue