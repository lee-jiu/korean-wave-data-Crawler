from TikTokApi import TikTokApi
import uuid
# import json 
from collections import OrderedDict
from dbconnect2 import db_con, insert_sns_page

conn = db_con()

file_data = OrderedDict()

verifyFp = "verify_kjrpy376_43tD17bY_o9AA_4HFS_8hsr_h8RUEw4aajy7"

api = TikTokApi.get_instance(custom_verifyFp=verifyFp, use_test_endpoints=True)

cookie = {
"s_v_web_id": "<O_Yjlp_4HOb_9btz_PMguaHPH05E9>",
"tt_webid": "<6986512204028757506>"
}

def tiktok_page():
   
    model_kwargs = {
       'biz_kind' : 'post',
       'source_name' : 'tiktok',
       'content_count' : None,
       'post_date' : None,
       'play_count' : None,
       'vote' : None,
       'comment_count' : None
    }

    count = 1000

  
    username_list = [
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
        'ahn_ellybaby',
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
        'p1harmony',
        'cjes.entertainment',
        'mbk.dia',
        'stonemusicent',
        'bornfreeonekisst',
        'vav_official_kr',
        'jacob.vav',
        'stvan.vav',
        'sambahong85',
        'hcy7102',
        'bravesound_',
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
        'onlyoneofofficial',
        'demian_isme',
        '2147947697',
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
        'kingdom_gfent',
        'ciipher_official'
      ]


    for user_n in username_list:
        try:
         tiktoks = api.by_username(str(user_n), count=count)

        except:
         continue


        # for tiktok_post in tiktoks:
        #     model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')
        #     # print(tiktok_post['id'])
        #     model_kwargs['content'] = tiktok_post['desc']
        #     model_kwargs['url'] = 'https://www.tiktok.com/@'+ tiktok_post['author']['uniqueId'] +'/video/'+ tiktok_post['id']
        #     model_kwargs['like_count'] = tiktok_post['stats']['diggCount']
        #     model_kwargs['view_count'] = tiktok_post['stats']['playCount']
        #     model_kwargs['share_count'] = tiktok_post['stats']['shareCount']
        #     model_kwargs['comment_count'] = tiktok_post['stats']['commentCount']
        #     model_kwargs['page_id'] = tiktok_post['author']['id']
        #     model_kwargs['page_name'] = tiktok_post['author']['uniqueId']
       

        for tiktok_page in tiktoks: 
            model_kwargs['data_id'] = str(uuid.uuid4()).replace('-','')
            model_kwargs['page_id'] = tiktok_page['author']['id'] 
            model_kwargs['url'] = 'https://www.tiktok.com/@'+ tiktok_page['author']['uniqueId'] 
            model_kwargs['page_id'] = tiktok_page['author']['id']
            model_kwargs['page_name'] = tiktok_page['author']['uniqueId']
            model_kwargs['page_nickname'] = tiktok_page['author']['nickname']
            model_kwargs['like_count'] = tiktok_page['authorStats']['heart']
            model_kwargs['follower'] = tiktok_page['authorStats']['followerCount']
            model_kwargs['follow'] = tiktok_page['authorStats']['followingCount']
            model_kwargs['video_count'] = tiktok_page['authorStats']['videoCount']
            
            
            insert_sns_page(conn=conn, model_kwargs=model_kwargs) 
            print(model_kwargs)
   
    conn.close()


tiktok_page()
