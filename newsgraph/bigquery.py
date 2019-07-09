from google.cloud import bigquery
import google
import json
from . import schemas


def build_schema_from_json(json_schema):
    json_schema = json.loads(json_schema)
    res = []
    for field in json_schema:
        res.append(bigquery.schema.SchemaField.from_api_repr(field))
    return res


def create_dataset_if_not_exists(client, dataset_name):
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
    except google.cloud.exceptions.NotFound:
        client.create_dataset(dataset_ref)


def create_table_if_not_exists(client, dataset_name, table_name, json_schema):
    create_dataset_if_not_exists(client, dataset_name)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)
    except google.cloud.exceptions.NotFound:
        table = bigquery.Table(table_ref, schema=build_schema_from_json(json_schema))
        table = client.create_table(table)


def get_table(client, dataset_name, table_name):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    return client.get_table(table_ref)


def insert_into_table(client, dataset_name, table_name, rows):
    table = get_table(client, dataset_name, table_name)
    errors = client.insert_rows(table, rows)  # API request
    assert errors == []


class BigqueryHelper():

    def __init__(self, config):
        self.client = bigquery.Client.from_service_account_json(config['SERVICE_ACCOUNT_PATH'])
        self.config = config
        self.articles_dataset = config['ARTICLES_DATASET']
        self.articles_table = config['ARTICLES_TABLE']
        create_table_if_not_exists(self.client,
                                   self.articles_dataset,
                                   self.articles_table,
                                   schemas.ARTICLES_TABLE_SCHEMA)

    def list_saved_articles(self, feed_url):
        query = """
            SELECT main_url
            FROM `{PROJECT_ID}.{ARTICLES_DATASET}.{ARTICLES_TABLE}`
            where feed_url="{feed_url}"
            LIMIT 1000
        """.format(**self.config, feed_url=feed_url)

        query_job = self.client.query(query, location="US")

        return [row['main_url'] for row in query_job]

    def insert_articles(self, articles):
        print('saving {} articles...'.format(len(articles)))
        if not articles:
            return
        insert_into_table(self.client, self.articles_dataset,
                          self.articles_table, articles)
