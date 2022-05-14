import re
import time
import mariadb
import unicodedata


host = '192.168.0.247'
port = 3306

user = 'crawlUser'
pw = 'password#1234'
db = 'crawl_wave'
# db = 'crawl_food'

conn = None
cur = None

sql = ""


def db_con():
    conn = mariadb.connect(host=host, port=port, user=user, password=pw, database=db)
    return conn


def get_keyword(conn, source_name, category, use_category=True):
    table_name = 'tb_keyword'
    cur = conn.cursor(buffered=True)
    if use_category:
        get_query = f'SELECT keyword_kr, keyword, category FROM {table_name} WHERE source_name =' + '%(source_name)s' + ' and category = %(category)s' + ' and use_yn = 1'
        cur.execute(get_query, {'source_name': source_name, 'category': category})
    else:
        get_query = f'SELECT keyword_kr, keyword, category FROM {table_name} WHERE source_name =' + '%(source_name)s' + ' and use_yn = 1'
        cur.execute(get_query, {'source_name': source_name})
    return cur


def exclude_text(model_kwargs):
    cnt = 0
    if model_kwargs['title'] != None or model_kwargs['title'] != '' and model_kwargs['content'] != None or model_kwargs['content'] != '':
        title_list = []
        content_list = []
        if model_kwargs['title'] != None:
            title_list = list(model_kwargs['title'])
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

def exclude_text_productreview(model_kwargs):
    cnt = 0
    if model_kwargs['title'] != None or model_kwargs['title'] != '' and model_kwargs['content'] != None or model_kwargs['content'] != '':
        # title_list = []
        content_list = []
        # if model_kwargs['title'] != None:
        #     title_list = list(model_kwargs['title'])
        if model_kwargs['content'] != None:
            content_list = list(model_kwargs['content'])

        for text in content_list:
            if re.match('[A-Z]', str(text)) != None or re.match('[a-z]', str(text)) != None:
                cnt += 1
        if  cnt >= 2:
            return False
        else:
            return True
    else:
        True

def duplicate_title(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    #dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(url)s)'# + ' COLLATE utf8_general_ci)'
    dup_query = f'SELECT COUNT(*) FROM {table_name} WHERE title =' + '%(title)s'

    try:
        model_kwargs['title'] = unicodedata.normalize('NFKD', model_kwargs['title'])
        cur.execute(dup_query, {'title': model_kwargs['title']})

        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            return True
        else:
            return False
    except mariadb.InterfaceError:
        return False
    except mariadb.Error:
        test_title = model_kwargs['title']
        print(f'SELECT * FROM {table_name} WHERE title = {test_title}')

def duplicate_sourcename(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    #dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(url)s)'# + ' COLLATE utf8_general_ci)'
    dup_query = f'SELECT COUNT(*) FROM {table_name} WHERE source_name =' + '%(source_name)s'

    try:
        model_kwargs['source_name'] = unicodedata.normalize('NFKD', model_kwargs['source_name'])
        cur.execute(dup_query, {'source_name': model_kwargs['source_name']})

        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            return True
        else:
            return False
    except mariadb.InterfaceError:
        return False
    except mariadb.Error:
        test_sourcename = model_kwargs['source_name']
        print(f'SELECT * FROM {table_name} WHERE source_name = {test_sourcename}')

def duplicate_writer(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    #dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(url)s)'# + ' COLLATE utf8_general_ci)'
    dup_query = f'SELECT COUNT(*) FROM {table_name} WHERE writer =' + '%(writer)s'

    try:
        #model_kwargs['writer'] = unicodedata.normalize('NFKD', model_kwargs['writer'])
        print(model_kwargs['writer'])
        cur.execute(dup_query, {'writer': model_kwargs['writer']})

        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            return True
        else:
            return False
    except mariadb.InterfaceError:
        return False
    except mariadb.Error:
        test_writer = model_kwargs['writer']
        print(f'SELECT * FROM {table_name} WHERE writer = {test_writer}')

def duplicate_content(conn, table_name, model_kwargs):
    cur = conn.cursor(buffered=True)
    #dup_query = f'SELECT EXISTS(SELECT * FROM {table_name} WHERE url =' + '%(url)s)'# + ' COLLATE utf8_general_ci)'
    dup_query = f'SELECT COUNT(*) FROM {table_name} WHERE content =' + '%(content)s'

    try:
        #model_kwargs['content'] = unicodedata.normalize('NFKD', model_kwargs['content'])
        cur.execute(dup_query, {'content': model_kwargs['content']})

        dup_count = cur.fetchone()[0]
        if dup_count > 0:
            return True
        else:
            return False
    except mariadb.InterfaceError:
        return False
    except mariadb.Error:
        test_content = model_kwargs['content']
        print(f'SELECT * FROM {table_name} WHERE content = {test_content}')

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


# 테스트 코드
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
                   'data_id, biz_kind, source_name, url, page_id, page_name, page_nickname, like_count, content_count, ' \
                   'follower, follow, post_date, play_count, video_count, comment_count, country, other_sns) ' \
                   'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

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
            model_kwargs['comment_count'],
            model_kwargs['country'],
            model_kwargs['other_sns']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()


def insert_sns_post(conn, model_kwargs):
    table_name = 'tb_sns_post'

    '''   if duplicate_url(conn, table_name, model_kwargs):
        print('dup_post')
        return False'''

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, biz_kind, category, source_name, keyword, keyword_kr, page_id, page_name, ' \
                   f'url, post_date, title, content, share_count, comment_count, like_count,' \
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
        print(model_kwargs['post_date'], 'exclude_news_post', model_kwargs['url'])
        return False

    if duplicate_title(conn, table_name, model_kwargs):
        if duplicate_content(conn, table_name, model_kwargs):
            print(model_kwargs['post_date'], 'duplicate_news_post', model_kwargs['url'])
            return False

    # if duplicate_title(conn, table_name, model_kwargs):
    #     if duplicate_sourcename(conn, table_name, model_kwargs):
    #         print('dup news')
    #         return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(biz_kind, category, data_id, source_name, url, keyword, keyword_kr, post_date, title, content, comment_count, country, methd, tag,happy ,unmoved, amused, excited, angry, sad, like_count, dislike_count) VALUES (?,?,?,?,?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

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
            model_kwargs['methd'],
            model_kwargs['tag'],
            model_kwargs['happy'],
            model_kwargs['unmoved'],
            model_kwargs['amused'],
            model_kwargs['excited'],
            model_kwargs['angry'],
            model_kwargs['sad'],
            model_kwargs['like_count'],
            model_kwargs['dislike_count']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print(model_kwargs['post_date'], 'insert_news_post', model_kwargs['url'])
    conn.commit()
    return True

def insert_news_comment(conn, model_kwargs):
    table_name = 'tb_news_comment'

    if duplicate_url(conn, table_name, model_kwargs):
        print('dup_comment')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(data_id, comment_id, source_name, url, writer, content, post_date, like_count, dislike_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['comment_id'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['writer'],
            model_kwargs['content'],
            model_kwargs['post_date'],
            model_kwargs['like_count'],
            model_kwargs['dislike_count']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    print('insert_comment')
    conn.commit()



def insert_product_post(conn, model_kwargs):
    table_name = 'tb_product_post'

    if exclude_text(model_kwargs=model_kwargs):
        print('exclude')
        return 'exclude'

    if duplicate_title(conn, table_name, model_kwargs):
        print('dup')
        return 'dup'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' \
                   '( data_id, keyword, biz_kind, source_name, title, manufacturer, category, prod_category, url, review_count, rating, content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['keyword'],
            model_kwargs['biz_kind'],
            model_kwargs['source_name'],
            model_kwargs['title'],
            model_kwargs['manufacturer'],
            model_kwargs['category'],
            model_kwargs['prod_category'],
            model_kwargs['url'],
            model_kwargs['review_count'],
            model_kwargs['rating'],
            model_kwargs['content']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_product_review(conn, model_kwargs):
    table_name = 'tb_product_review'

    if exclude_text_productreview(model_kwargs=model_kwargs):
        print('exclude review')
        return

    # if duplicate_url(conn, table_name, model_kwargs):
    #     print('dup_review')
    #     return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' \
                   '( data_id, comment_id, source_name, writer, content, like_count, post_date, star_point, country, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['comment_id'],
            model_kwargs['source_name'],
            model_kwargs['writer'],
            model_kwargs['content'],
            model_kwargs['like_count'],
            model_kwargs['post_date'],
            model_kwargs['star_point'],
            model_kwargs['country'],
            model_kwargs['url']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_content_review(conn, model_kwargs):
    table_name = 'tb_content_review'

    if exclude_text_productreview(model_kwargs=model_kwargs):
        print('exclude review')
        return

    if duplicate_content(conn, table_name, model_kwargs):
        if duplicate_writer(conn, table_name, model_kwargs):
            print('dup_review')
            return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' \
                   '(comment_id, source_name, category, title, writer, review_title, content, like_count, post_date, star_point_five, star_point_ten, url, biz_kind, global_category) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['comment_id'],
            model_kwargs['source_name'],
            model_kwargs['category'],
            model_kwargs['title'],
            model_kwargs['writer'],
            model_kwargs['review_title'],
            model_kwargs['content'],
            model_kwargs['like_count'],
            model_kwargs['post_date'],
            model_kwargs['star_point_five'],
            model_kwargs['star_point_ten'],
            model_kwargs['url'],
            model_kwargs['biz_kind'],
            #model_kwargs['methd'],
            model_kwargs['global_category']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_news_comment(conn, model_kwargs):
    table_name = 'tb_news_comment'

    if duplicate_url(conn, table_name, model_kwargs):
        print('dup_post')
        return

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name}' + \
                   '(data_id, comment_id, source_name, url, writer, content, post_date, like_count, dislike_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['comment_id'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['writer'],
            model_kwargs['content'],
            model_kwargs['post_date'],
            model_kwargs['like_count'],
            model_kwargs['dislike_count']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def get_video_data(conn, category, post_date1, post_date2, source_name, create_at):
    table_name = 'tb_sns_post'
    cur = conn.cursor(buffered=True)
    get_query = f'SELECT data_id, url FROM {table_name} WHERE category =? and left(post_date,10) >= ? and left(post_date,10) <= ? and source_name = ?' \
                f' and left(create_at, 10) <= ?' \


    cur.execute(get_query, (category, post_date1, post_date2, source_name, create_at))
    return cur

def return_comment_count(conn, data_id, source_name):
    table_name = 'tb_sns_comment'
    cur = conn.cursor(buffered=True)
    get_query = f'SELECT count(comment_id) FROM {table_name} WHERE data_id = ? and source_name = ?'

    cur.execute(get_query, (data_id, source_name))
    return cur

def update_post(conn, model_kwargs):
    table_name = 'tb_sns_post'

    cur = conn.cursor(buffered=True)
    insert_query = f'UPDATE {table_name} set page_name = ?, title = ?, content = ?, comment_count = ?, like_count = ?, ' \
                   f'dislike_count = ?, view_count = ?, create_at = current_timestamp()' \
                   f' where data_id = ? and source_name = ? and url = ? and page_id = ?'


def insert_restaurant_post(conn, model_kwargs):
    table_name = 'tb_restaurant_post'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, keyword, biz_kind, domain, title, address, url, review_count, ' \
                   f'rating, rating_food, rating_service, rating_value, rating_atmosphere)' \
                   f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['page_name'],
            model_kwargs['title'],
            model_kwargs['content'],
            model_kwargs['comment_count'],
            model_kwargs['like_count'],
            model_kwargs['dislike_count'],
            model_kwargs['view_count'],
            model_kwargs['data_id'],
            model_kwargs['source_name'],
            model_kwargs['url'],
            model_kwargs['page_id']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['keyword'],
            model_kwargs['biz_kind'],
            model_kwargs['domain'],
            model_kwargs['title'],
            model_kwargs['address'],
            model_kwargs['url'],
            model_kwargs['review_count'],
            model_kwargs['rating'],
            model_kwargs['rating_food'],
            model_kwargs['rating_service'],
            model_kwargs['rating_value'],
            model_kwargs['rating_atmosphere']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_restaurant_review(conn, model_kwargs):
    table_name = 'tb_restaurant_review'

    cur = conn.cursor(buffered=True)
    insert_query = f'INSERT INTO {table_name} (data_id, comment_id, writer, title, content, like_count, ' \
                   f'post_date, visit_date, star_point)' \
                   f' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'

    try:
        cur.execute(insert_query, (
            model_kwargs['data_id'],
            model_kwargs['comment_id'],
            model_kwargs['writer'],
            model_kwargs['title'],
            model_kwargs['content'],
            model_kwargs['like_count'],
            model_kwargs['post_date'],
            model_kwargs['visit_date'],
            model_kwargs['star_point']
        ))

    except mariadb.Error as e:
        print(f'Error: {e}')

    conn.commit()

def insert_post_url(conn, model_kwargs):
    url_table = 'tb_post_url'

    cur = conn.cursor(buffered=True)


    model_kwargs['url'] = unicodedata.normalize('NFKD', model_kwargs['url'])

    url = model_kwargs['url']

    insert_url = f'INSERT INTO tb_post_url (url) VALUES (\'{url}\')'
    print(insert_url)
    try:
        cur.execute(insert_url)
        print('insert_url')

    except mariadb.Error as e:
        print(f'{url_table} Error: {e}')
    conn.commit()