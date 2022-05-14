import mariadb
import unicodedata
import re
import time

# # 외부망
# host = 'data.misoinfo.co.kr'
# port = 24733

#내부망
host = '192.168.0.247'
port = 3306

user = 'crawlUser'
pw = 'password#1234'
db = 'crawl_wave'

conn = None
cur = None

sql = ""
def db_con():
    conn = mariadb.connect(host=host, port=port, user=user, password=pw, database=db)
    return conn


def duplicate_date(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(post_date)s)' + 'and title =' + '%(title)s)' #COLLATE utf8_general_ci)'
    try:
        model_kwargs['date'] = unicodedata.normalize('NFKD', model_kwargs['date'])
        model_kwargs['date'] = unicodedata.normalize('NFKD', model_kwargs['date'])
        cur.execute(dup_query, {'date': model_kwargs['date']})
    except mariadb.InterfaceError:
        return False
    return cur.fetchone()[0]

def get_keyword(conn, source_name, category):
    table_name = 'tb_keyword'
    cur = conn.cursor(buffered=True)
    get_query = f'SELECT keyword_kr, keyword FROM {table_name} WHERE source_name =? and category =? and use_yn = 1'

    cur.execute(get_query, (source_name, category))
    return cur

def exclude_text(model_kwargs):
    cnt = 0
    if model_kwargs['content'] != None or model_kwargs['content'] != '':
        title_list = []
        content_list = []
        if model_kwargs['content'] != None:
            content_list = list(model_kwargs['content'])
        texts = title_list + content_list
        for text in texts:
            if re.match('[A-Z]', str(text)) != None or re.match('[a-z]', str(text)) != None:
                cnt += 1
        if  cnt >= 2:
            return False
        else:
            return True
    else:
        True

def duplicate_url(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    #dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(url)s)'# + ' COLLATE utf8_general_ci)'
    dup_query = f'SELECT COUNT(*) FROM {table_name} WHERE url =' + '%(url)s'
    try:
        model_kwargs['url'] = unicodedata.normalize('NFKD', model_kwargs['url'])
        cur.execute(dup_query, {'url': model_kwargs['url']})

        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            return True
        else:
            return False
    except mariadb.InterfaceError:
        return False
    except mariadb.Error:
        test_url = model_kwargs['url']
        print(f'SELECT * FROM {table_name} WHERE url = {test_url}')


def insert_test_reddit(conn, model_kwargs):
    table_name = 'tb_test_reddit'

    if duplicate_url(conn, table_name, model_kwargs):
        print('dup')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (source_name, title, post_date, vote, author, view_count, comment_count, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['source_name'],
            model_kwargs['title'],
            model_kwargs['post_date'],
            model_kwargs['vote'],
            model_kwargs['author'],
            model_kwargs['view_count'],
            model_kwargs['comment_count'],
            model_kwargs['url']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()


def insert_sns_page(conn, model_kwargs):
    table_name = 'tb_sns_page'

    if duplicate_url(conn, table_name, model_kwargs):
        print('dup_page')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (' \
                   f'data_id, biz_kind, source_name, url, page_id, page_name, page_nickname, like_count, content_count, ' \
                   f'follower, follow, post_date, play_count, video_count, ' \
                   f'comment_count) ' \
                   f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['page_id'],
            model_kwargs['page_name'],
            model_kwargs['page_nickname'],
            model_kwargs['like_count'],
            model_kwargs['content_count'],
            model_kwargs['follower'],
            model_kwargs['follow'],
            model_kwargs['post_date'],
            model_kwargs['play_count'],
            model_kwargs['video_count'],
            model_kwargs['comment_count']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_sns_post(conn, model_kwargs):
    table_name = 'tb_sns_post'

    if duplicate_url(conn, table_name, model_kwargs):
        print('dup_post')
        return False

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, biz_kind, category, source_name, keyword, keyword_kr, page_id, page_name, ' \
                   f'url, post_date, title, content, share_count, comment_count, like_count, '\
                   f'retweet_count, typ, dislike_count, view_count, vote, methd)' \
                   f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'],
            model_kwargs['category'],
            model_kwargs['source_name'],
            model_kwargs['keyword'],
            model_kwargs['keyword_kr'],
            model_kwargs['page_id'],
            model_kwargs['page_name'],
            model_kwargs['url'],
            model_kwargs['post_date'],
            model_kwargs['title'],
            model_kwargs['content'],
            model_kwargs['share_count'],
            model_kwargs['comment_count'],
            model_kwargs['like_count'],
            model_kwargs['retweet_count'],
            model_kwargs['typ'],
            model_kwargs['dislike_count'],
            model_kwargs['view_count'],
            model_kwargs['vote'],
            model_kwargs['methd']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()
    return True

def insert_sns_comment(conn, model_kwargs):
    table_name = 'tb_sns_comment'
    # 저장할 테이블의 이름

    if exclude_text(model_kwargs=model_kwargs):
        print('exclude_text')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (' \
                   f'data_id, comment_id, source_name, url, writer, content, like_count,' \
                   f'post_date) ' \
                   f'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    # 해당 테이블의 column명 다 넣어야합니다 VALUES는 column의 갯수와 똑같이 해야합니다

    try:
        # 해당 테이블의 column명을 다 넣어야합니다
        print('insert_comment')
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['comment_id'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['writer'],
            model_kwargs['content'],
            model_kwargs['like_count'],
            model_kwargs['post_date']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()


def insert_news_search_post(conn, model_kwargs):
    table_name = 'tb_news_search_post'

    if exclude_text(model_kwargs=model_kwargs):
        return
    if duplicate_url(conn, table_name, model_kwargs):
        print('dup_post')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(biz_kind, category, data_id, source_name, url, keyword, keyword_kr, post_date, title, content, comment_count, country, method, tag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['biz_kind'],
            model_kwargs['category'],
            model_kwargs['data_id'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['keyword'],
            model_kwargs['keyword_kr'],
            model_kwargs['post_date'],
            model_kwargs['title'],
            model_kwargs['content'],
            model_kwargs['comment_count'],
            model_kwargs['country'],
            model_kwargs['method'],
            model_kwargs['tag']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()


def insert_keyword(conn, model_kwargs):
    table_name = 'tb_keyword'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(source_name, category, keyword_kr, keyword, use_yn) VALUES (?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['source_name'],
            model_kwargs['category'],
            model_kwargs['keyword_kr'],
            model_kwargs['keyword'],
            model_kwargs['use_yn'],
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()

def insert_country_rank(conn, model_kwargs):
    table_name = 'tb_content_country_rank'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(data_id, biz_kind, source_name, crawl_date, title, com_platform, country, date, rank)' \
                   ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'],
            model_kwargs['source_name'],
            model_kwargs['crawl_date'],
            model_kwargs['title'],
            model_kwargs['com_platform'],
            model_kwargs['country'],
            model_kwargs['date'],
            model_kwargs['rank']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()

def insert_content_daily_rank(conn, model_kwargs):
    table_name = 'tb_content_daily_rank'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(data_id, biz_kind, source_name, post_date, title, com_platform, movie_rank, total_point, create_at, url)' \
                   ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'] ,
            model_kwargs['source_name'],
            model_kwargs['post_date'],
            model_kwargs['title'],
            model_kwargs['com_platform'],
            model_kwargs['movie_rank'],
            model_kwargs['total_point'],
            model_kwargs['create_at'],
            model_kwargs['url']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()

def insert_content_meta(conn, model_kwargs):
    table_name = 'tb_content_meta'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, biz_kind, source_name, post_date, content_rank, ' \
                   f'title, artist_name, director_name, content_type, country, stream_platform, genre, ' \
                   f'detailed_genre, content, url, release_date, rotten_tomatoes_rating, IMDB_rating) ' \
                   f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'],
            model_kwargs['source_name'],
            model_kwargs['post_date'],
            model_kwargs['content_rank'],
            model_kwargs['title'],
            model_kwargs['artist_name'],
            model_kwargs['director_name'],
            model_kwargs['content_type'],
            model_kwargs['country'],
            model_kwargs['stream_platform'],
            model_kwargs['genre'],
            model_kwargs['detailed_genre'],
            model_kwargs['content'],
            model_kwargs['url'],
            model_kwargs['release_date'],
            model_kwargs['rotten_tomatoes_rating'],
            model_kwargs['IMDB_rating']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()

def insert_content_rank(conn, model_kwargs):
    table_name = 'tb_content_rank'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, biz_kind, source_name, crawl_date, title, ' \
                   f'com_platform, total_date, country_trend_point, daily_trend_point, country) ' \
                   f'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['biz_kind'],
            model_kwargs['source_name'],
            model_kwargs['crawl_date'],
            model_kwargs['title'],
            model_kwargs['com_platform'],
            model_kwargs['total_date'],
            model_kwargs['country_trend_point'],
            model_kwargs['daily_trend_point'],
            model_kwargs['country']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs)
    conn.commit()

