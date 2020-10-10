import psycopg2
import requests
import yaml
import os
from bs4 import BeautifulSoup
import datetime


class Crawler:

    def __init__(self, conn_dict, server):

        self.conn_dict = conn_dict
        self.server = server

    def make_db_connection(self, autocommit=False):

        conn = psycopg2.connect(user=self.conn_dict[server]['user'], password=self.conn_dict[server]['password'],
                                host=self.conn_dict[server]['host'], dbname=self.conn_dict[server]['dbname'])
        cursor = conn.cursor()

        if autocommit:
            conn.autocommit = True

        return conn, cursor

    def prepare_table_search_results(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS public.search_result ("
                            "id int8 NOT NULL, "
                            "article_datetime date NULL, "
                            "article_timestamp int8 NULL, "
                            "category varchar NULL, "
                            "access_control varchar NULL, "
                            "headline varchar NULL, "
                            "link varchar NULL, "
                            "got_single bool NULL);")
        self.conn.commit()
        print('table created!')

        self.cursor.execute("DROP SEQUENCE IF EXISTS id_seq_search_result CASCADE; "
                            "CREATE SEQUENCE id_seq_search_result; "
                            "alter table search_result alter column id set default nextval('id_seq_search_result')")
        self.conn.commit()

        self.cursor.execute("ALTER TABLE search_result ADD CONSTRAINT search_result_un UNIQUE (link);")
        self.conn.commit()

    @staticmethod
    def get_search_results(page):

        url = "https://www.autonews.com/news?type=article&field_emphasis_image=&page=" + str(page)

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        response = response.text.encode('utf8')

        return response

    # @staticmethod
    def parse_search_results(self, response):
        soup = BeautifulSoup(response, 'html.parser')
        del response

        articles = soup.findAll("div", {"class": "views-row section-front-row"})
        # print(articles)
        if len(articles) == 0:
            articles = soup.findAll("div", {"class": "views-row section-front-row load-more-page"})

            if len(articles) == 0:
                print('whole data crawling completed')
                return False
        print(len(articles))

        for article in articles:

            access_control = article.find("div", {"class": "feature-article-access-control"})
            try:
                access_control = access_control.text.strip()
            except:
                access_control = None

            category_timestamp = article.find("div", {"class": "feature-article-category-timestamp"})
            category = category_timestamp.span.text.strip()
            timestamp = category_timestamp.find("span",
                                                {"class": "text-gray article-update-time divider-gray"})
            timestamp = int(timestamp['data-lastupdated'].split('--')[1].strip())
            article_datetime = datetime.datetime.fromtimestamp(timestamp)
            headline = article.find('div', {'class': 'feature-article-headline'})
            link = headline.a.get('href')
            headline = headline.text.strip()

            dic = {'access_control': access_control, 'category': category, 'timestamp': timestamp,
                   'article_datetime': article_datetime, 'headline': headline, 'link': link}

            self.insert_search_results(dic=dic)
        return True

    def insert_search_results(self, dic):

        try:
            self.cursor.execute("insert into search_result (article_datetime, article_timestamp, category, "
                                "access_control, headline, link) values (%s,%s,%s,%s,%s,%s)",
                                (dic['article_datetime'], dic['timestamp'], dic['category'], dic['access_control'],
                                 dic['headline'], dic['link']))
        except Exception as e:
            print(e)

    def get_single_page(self):
        pass

    def parse_single_page(self):
        pass

    def insert_single_page(self):
        pass

    def search_pipeline(self, autocommit=False, prepare_table_search_results=False):

        self.conn, self.cursor = self.make_db_connection(autocommit=autocommit)

        if prepare_table_search_results:
            self.prepare_table_search_results()

        page = 0
        while True:
            print('page_number:', page)
            response = self.get_search_results(page=page)
            state = self.parse_search_results(response=response)
            if state == False:
                break
            page += 1

    def single_page_pipeline(self):
        pass


if __name__ == '__main__':

    ROOT_DIR = str(str(os.path.realpath(__file__).replace('\\', '/')).split('sample_task/')[0]) + 'sample_task/'
    conf_dir = ROOT_DIR + 'config/db_configs.yaml'
    conn_dict = yaml.load(open(conf_dir))

    server = 'local'

    crawler = Crawler(conn_dict=conn_dict, server=server)
    crawler.search_pipeline(prepare_table_search_results=False, autocommit=True)

