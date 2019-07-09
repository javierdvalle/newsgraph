from google.cloud import datastore
import datetime


class DatastoreHelper():

    def __init__(self, config):
        self.client = datastore.Client.from_service_account_json(config['SERVICE_ACCOUNT_PATH'])
        self.config = config
        self.feeds_kind = config['FEEDS_KIND']

    @staticmethod
    def build_new_rss_feed(feed_url):
        return {
            'feed_type': 'rss',
            'feed_url': feed_url,
            'created_at': datetime.datetime.utcnow(),
            'updated_at': None,
            'articles_count': 0,
            'last_articles_added_at': None,
            'last_articles_count': 0,
        }

    def add_rss_feed(self, feed_url):
        key = self.client.key(self.feeds_kind, feed_url)  # urllib.parse.quote_plus(feed_url)
        entity = self.client.get(key)  # API call

        if entity is None:
            entity = datastore.Entity(key)
            # entity = datastore.Entity(key, exclude_from_indexes=['description'])
            entity.update(DatastoreHelper.build_new_rss_feed(feed_url))
        else:
            pass

        self.client.put(entity)
        return entity

    def get_feeds(self):
        query = self.client.query(kind=self.feeds_kind)
        query.order = ['created_at']
        return list(query.fetch())

    def update_feed(self, feed_url, old_articles_count, new_articles_count):
        key = self.client.key(self.feeds_kind, feed_url)
        entity = self.client.get(key)  # API call
        if entity is not None:
            updates = {}
            now = datetime.datetime.utcnow()
            updates['updated_at'] = now
            updates['articles_count'] = old_articles_count + new_articles_count
            if new_articles_count > 0:
                updates['last_articles_count'] = new_articles_count
                updates['last_articles_added_at'] = now
            entity.update(updates)
            self.client.put(entity)
        return entity


if __name__ == '__main__':
    from .config_utils import load_instance

    config = load_instance('/home/javier/wkf/newsgraph/instances/instance1')
    ds_helper = DatastoreHelper(config)
    print(ds_helper.get_feeds())
