from TikTokApi import TikTokApi
import uuid
# import json 
from collections import OrderedDict
from datetime import datetime, date
from dbconnect2 import db_con, insert_sns_post
import time

conn = db_con()

file_data = OrderedDict()

verifyFp = "verify_kjrpy376_43tD17bY_o9AA_4HFS_8hsr_h8RUEw4aajy7"

# api = TikTokApi.get_instance(custom_verifyFp=verifyFp, use_test_endpoints=True, proxy="213.137.240.243:81")
# api = TikTokApi.get_instance(proxy="119.4.167.141:80")

api = TikTokApi.get_instance(custom_verifyFp=verifyFp, use_test_endpoints=True)


cookie = {
"s_v_web_id": "<O_Yjlp_4HOb_9btz_PMguaHPH05E9>",
"tt_webid": "<6986512204028757506>"
}

# cookie = {
# "s_v_web_id": "<verify_kw00nily_331SKK0F_qGhg_493a_BInv_4Ux0AKvNHl1P>",
# "ttwid": "<1%7CnjDO-eyAeXaF8IgWAuVlmmn5aGgAlELrPMn3c-z-DW8%7C1638855563%7Cc40a01756fb58ed56df1828aa11db9079e3a27173a212dd627635643d054ea3d>"
# }


def tiktok_post():
    
    model_kwargs = {
       'biz_kind' : 'video',
       'category' : 'ARTIST',
       'source_name' : 'tiktok',
       'keyword' : None,
       'keyword_kr' : None,
       'title': None,
       'retweet_count' : None,
       'typ' : 'video',
       'dislike_count' : None,
       'vote' : None,
       'methd' : 'page'
    }


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

    count = 1000

    # username_list = [
    # 'official_gidle', 'ab6ix.official', 'aespa_official', 'astro_official',
    # 'bambamxabyss', 'bi_131_bi', 'bp_tiktok', 'bts_official_bighit', 'chaelincl', 'cube_clc_official',
    # 'official_dreamcatcher', 'enhypen', 'everglowofficial', 'hyunaofficial', 'itzyofficial', 'jacksonwang'
    # 'lovelyz_official', 'momoland_161110', 'monsta_x_514', 'official_nct', 'wm_ohmygirl', 'rbw_oneus',
    # 'official_ptg', 'redvelvet_smtown', 'roses_are_rosie',     
    # 'seventeen17_official',
    # 'sf9official', 'shinee_official',
    # 'somi_official_', 'stayc_official', 'jypestraykids', 'official_sunmi', 'superjunior_smtown', 'txt.bighitent',
    # 'twice_tiktok_official', 'official_wayv', 'weeekly', 'official_wonho' 
    #   ]
    username_list = [
        'official_gidle', 
        'ab6ix.official', 
        'aespa_official', 
        'astro_official',
        'bambamxabyss', 
        'bi_131_bi', 
        'bp_tiktok', 
        'bts_official_bighit', 
        'chaelincl', 
        'cube_clc_official',
        'official_dreamcatcher', 
        'enhypen', 
        'everglowofficial', 
        'hyunaofficial',
        'itzyofficial', 
        'jacksonwang',
        'lovelyz_official', 
        'momoland_161110', 
        'monsta_x_514', 
        'official_nct', 
        'wm_ohmygirl', 
        'rbw_oneus',
        'official_ptg', 
        'redvelvet_smtown', 
        'roses_are_rosie',     
        'seventeen17_official',
        'sf9official', 
        'shinee_official',
        'somi_official_', 
        'stayc_official', 
        'jypestraykids', 
        'official_sunmi', 
        'superjunior_smtown', 
        'txt.bighitent',
        'twice_tiktok_official', 
        'official_wayv', 
        'weeekly', 
        'official_wonho',
        'got7official',
        'smtown_official',
        'ikon_tiktok',
        'onlyoneofofficial',
        'p1harmony',
        'nct127_loveholic',
        'wn_tiktok',
        '0529.jihoon',
        'official_btob',
        'official_apink2011',
        'jypark_official',
        'official_gfriend',
        'ahn_ellybaby'
        'hyeliniseo823',
        'ateez_official_',
        'jun2dakay_official',
        'm2mpd',
        'yg_treasure_tiktok',
        'officializone_',
        'day6_official',
        'creker_theboyz',
        'nuest_official',
        'official.kard',
        'official_b1a4',
        'ericnam',
        'mnet_tiktok_official',
        'jessica.syj',
        'hatfelt_official',
        'official_teentop',
        'starship_ent',
        'itsjessibaby',
        'superm_smtown',
        'akmu_suhyun',
        'up10tion__',
        'official_mamamoo',
        'nflyingofficial',
        'official_wjsn',
        'dayomi99_',
        'loonatheworld_official',
        'aomgofficial',
        'official.jbj95',
        'leehi_hi',
        'konnect_kangdaniel',
        'goldenchildofficial',
        'kjh_official',
        'epikhighishere',
        'wm_official',
        'cix_official',
        'cravityofficial',
        'official_fromis9',
        'rain.xix',
        'cjes.entertainment',
        'mbk.dia',
        'stonemusicent',
        'bornfreeonekisst',
        'vav_official_kr',
        'jacob.vav',
        'stvan.vav',
        'sambahong85',
        'hcy7102',
        'woodz_9696',
        'mcndofficial_',
        'jinseok_98',
        'youngjaexars',
        '_minzy_mz',
        'im_taemin',
        '24k__official',
        'official.onf',
        'official_mirae',
        'dohyon_tony',
        'yg_kplus_official',
        'official_victon1109',
        'samuelkimofficial',
        'official_chungha',
        'arirang_kpop',
        'simba_jjcc',
        'to1_offcl',
        'official_unb',
        'the_verivery',
        'wei__official',
        'musik0120',
        'official_yooseonho',
        'wearedrippin',
        'cherrybulletofficial',
        'youngji_02',
        'rbw_onewe',
        'weeekly',
        'oui_ent',
        'kozico0914',
        'official.jbj95',
        'official_rocketpunch',
        '1the9',
        'officiallaboum',
        'map6official',
        'hyominnn5',
        'ljh_official_',
        'demian_isme',
        'amoebakorea',
        'justb_official',
        'blackswan__official',
        'epex.official',
        'jiyeon2_',
        'iamhenry',
        'h1ghrmusic_official',
        'woooojinn',
        'sbsloud',
        'thunderpark7',
        'gwsn.official',
        '_hyolyn_',
        'okdal_official',
        'berrygood_official',
        'officialparkbom',
        't1419_official',
        'official_d1ce',
        'dindinem',
        'keyeastofficial',
        'official_uni.t',
        'samuelkimofficial',
        'elast.official',
        'iz__official',
        'rbw_purplekiss',
        'official_kwoneunbi',
        'official.dkb',
        'saram_ent',
        'noir_official',
        'elris_official',
        'bravegirls_official',
        'tst_official',
        'kqentertainment',
        'hwangemo',
        'kingdom_gfent'
        'ciipher_official'
      ]

    for user_n in username_list:
        try:
         tiktoks = api.by_username(str(user_n), count=count)

        except:
         continue


        for tiktok_post in tiktoks:
            model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')
            # print(tiktok_post['id'])
            model_kwargs['content'] = tiktok_post['desc']
            model_kwargs['url'] = 'https://www.tiktok.com/@'+ tiktok_post['author']['uniqueId'] +'/video/'+ tiktok_post['id']
            model_kwargs['like_count'] = tiktok_post['stats']['diggCount']
            model_kwargs['view_count'] = tiktok_post['stats']['playCount']
            model_kwargs['share_count'] = tiktok_post['stats']['shareCount']
            model_kwargs['comment_count'] = tiktok_post['stats']['commentCount']
            model_kwargs['page_id'] = tiktok_post['author']['id']
            model_kwargs['page_name'] = tiktok_post['author']['uniqueId']

            date_ = datetime.fromtimestamp(int(tiktok_post['createTime'])).date()  
            if date_ > date(2021, 11, 30) :
                continue
            if date_ < date(2021, 11, 1) :
                continue
            if date_ < date(2021, 12, 1) and date_ > date(2021, 10, 31) :
                model_kwargs['post_date'] = date_

            # date_ = datetime.fromtimestamp(int(tiktok_post['createTime'])).date()  
            # if date_ > date(2021, 9, 30) :
            #     continue
            # if date_ < date(2021, 1, 1) :
            #     continue
            # if date_ < date(2021, 10, 1) and date_ > date(2020, 12, 31) :
            #     model_kwargs['post_date'] = date_    
       

            insert_sns_post(conn=conn, model_kwargs=model_kwargs) 
            print(model_kwargs)

            time.sleep(2)

    


    conn.close()





    # for tiktok_page in tiktoks:  
    #   print(tiktok_page['author']['id'])
    #   print(tiktok_page['author']['uniqueId'])
    #   print(tiktok_page['author']['nickname'])
    #   print(tiktok_page['authorStats']['heart'])
    #   print(tiktok_page['authorStats']['followerCount'])
    #   print(tiktok_page['authorStats']['followingCount'])
    #   print(tiktok_page['authorStats']['videoCount'])
   

tiktok_post()























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