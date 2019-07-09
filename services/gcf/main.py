import requests
from flask import jsonify
from newsgraph.bigquery import BigqueryHelper
from newsgraph.datastore import DatastoreHelper
from newsgraph.config_utils import load_instance
from newsgraph.feed import download_feed_articles

config = load_instance()
bq_helper = BigqueryHelper(config)
ds_helper = DatastoreHelper(config)


def hello(request):
    return jsonify({'hello': 'world'})


def update_feed(request):
    feed_url = request.json.get('feed_url')
    if not feed_url:
        return jsonify({'error': 'missing parameter: feed_url'})

    past_urls = bq_helper.list_saved_articles(feed_url)
    print('found {} urls of that feed'.format(len(past_urls)))

    articles = download_feed_articles(feed_url, skip_urls=past_urls)

    # save articles to bigquery
    bq_helper.insert_articles(articles)

    # update feed data in datastore
    feed = ds_helper.update_feed(feed_url,
                                 old_articles_count=len(past_urls),
                                 new_articles_count=len(articles))

    return jsonify({'existing_articles': len(past_urls),
                    'new_articles': len(articles),
                    'feed': dict(feed) if feed else None})


def add_rss_feed(request):
    feed_url = request.json.get('feed_url')
    if feed_url is None:
        return jsonify({'error': 'must provide a <feed_url>'})

    entity = ds_helper.add_rss_feed(feed_url)
    return jsonify(dict(entity))


def list_feeds(request):
    feed_list = ds_helper.get_feeds()
    return jsonify({'feed_list': feed_list})


def trigger_update_feeds(request):
    feed_list = ds_helper.list_feeds()
    feed_urls = [feed['feed_url'] for feed in feed_list if 'feed_url' in feed]
    for feed_url in feed_urls:
        json_payload = {'feed_url': feed_url}
        try:
            requests.post(config['UPDATE_FEED_API'], json=json_payload, timeout=0.1)
        except requests.exceptions.ReadTimeout:
            pass
    return jsonify({'feeds_count': len(feed_list), 'feeds': feed_list})


if __name__ == '__main__':
    from flask import Flask, request
    app = Flask(__name__)

    @app.route("/", methods=['GET', 'POST'])
    def flask_hello():
        return hello(request)

    @app.route("/update_feed", methods=['GET', 'POST'])
    def flask_update_feed():
        return update_feed(request)

    @app.route("/add_rss_feed", methods=['GET', 'POST'])
    def flask_add_feed():
        return add_rss_feed(request)

    @app.route("/list_feeds", methods=['GET', 'POST'])
    def flask_list_feeds():
        return list_feeds(request)

    @app.route("/trigger_update_feeds", methods=['GET', 'POST'])
    def flask_update_feeds():
        return trigger_update_feeds(request)

    app.run(port=8080)
