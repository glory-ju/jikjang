import configparser
import zmq
from loguru import logger
import pymysql
from rerestart import main_preprocessig_content

ctx = zmq.Context()
config = configparser.ConfigParser()
config.read('./news.cfg', encoding='utf8')

def conn_mariadb(config):
    try:
        conn = pymysql.connect(host=config['mariaDB']['hostname'],
                               user=config['mariaDB']['username'],
                               password=config['mariaDB']['password'],
                               db=config['mariaDB']['database'],
                               charset=config['mariaDB']['charset'])
        logger.success('DB connection success')
        return conn
    except Exception as ee:
        logger.error(f'DB connection Error: {ee}')

def run_server(port=61595):
    sock = ctx.socket(zmq.PULL)
    sock.bind(f'tcp://127.0.0.1:{port}')
    logger.info(f'STARTING SERVER AT {port}')
    conn = conn_mariadb(config)
    cursor = conn.cursor()
    query = '''
                    CREATE TABLE IF NOT EXISTS logan_test1(
                    objectID varchar(24) not null primary key,
                    date varchar(8),
                    headline varchar(1024),
                    category varchar(100),
                    press varchar(100),
                    content text,
                    url varchar(512))
                    '''
    cursor.execute(query)
    logger.info('mariaDB CONNECTION & create TABLE ::: SUCCESS!!')

    while True:
        I_will_send_mariadb = sock.recv_json()
        preprocessed_content = main_preprocessig_content(I_will_send_mariadb)
        insert_query = f"""
        INSERT INTO logan_test1 (objectID, date, headline, category, press, content, url)
        VALUES ("{I_will_send_mariadb['_id']}", "{I_will_send_mariadb['news_date']}", "{I_will_send_mariadb['headline']}",
        "{I_will_send_mariadb['subcategory']}", "{I_will_send_mariadb['press']}", "{preprocessed_content}", "{I_will_send_mariadb['content_url']}")
        """
        try:
            cursor.execute(insert_query)
            conn.commit()
        except Exception as eee:
            logger.error(eee)

        logger.info(insert_query)
    conn.close()
    logger.complete()

if __name__=='__main__':
    run_server()