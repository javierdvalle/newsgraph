import feedparser
import newspaper
import json
import datetime
from . import schemas


def convert_article_to_dict(article):
    attributes = ['additional_data', 'article_html', 'authors', 'canonical_link',
                  'download_exception_msg', 'download_state', 'html', 'images', 'imgs', 'keywords',
                  'link_hash', 'meta_data', 'meta_description', 'meta_favicon', 'meta_img',
                  'meta_keywords', 'meta_lang', 'meta_site_name', 'movies', 'publish_date',
                  'source_url', 'summary', 'tags', 'text', 'title', 'top_image', 'top_img', 'url']
    d = {}
    for attr in attributes:
        if hasattr(article, attr):
            d[attr] = getattr(article, attr)
        else:
            d[attr] = None
    return d


def download_article(url):
    article = newspaper.Article(url)
    article.download()
    article.parse()
    # print(article.text)
    return convert_article_to_dict(article)


def enrich_article_with_rss_data(article, rss_entry):
    article['rss_link'] = str(rss_entry.get('link'))
    article['rss_title'] = str(rss_entry.get('title'))
    article['rss_author'] = str(rss_entry.get('author'))
    article['rss_authors'] = [author['name'] for author in rss_entry.get('authors', [])]
    article['rss_summary'] = str(rss_entry.get('summary'))
    article['rss_published'] = str(rss_entry.get('published'))
    article['rss_tags'] = [tag['term'] for tag in rss_entry.get('tags', [])]
    article['rss_raw_data'] = str(rss_entry)


def build_bq_row(article, table_schema_json, skip_fields=['html']):
    table_schema = json.loads(table_schema_json)
    row = {}
    for field in table_schema:
        if field['name'] in skip_fields:
            continue
        if field['mode'] == 'REPEATED':
            row[field['name']] = list(article.get(field['name'], []))
        elif field['type'] == 'STRING':
            row[field['name']] = str(article.get(field['name']))
        else:
            row[field['name']] = article.get(field['name'])
    return row


def download_feed_articles(feed_url, skip_urls=[]):
    # TODO: sacar de bigquery la lista de links que ya descargue en el pasado, y no volver a hacerlo
    now = str(datetime.datetime.now())

    print('downloading rss feed...')
    feed = feedparser.parse(feed_url)
    rows = []

    print('downloading feed entries...')
    for entry in feed.entries:
        try:
            if entry.link in skip_urls:
                print('skipping url: {}'.format(entry.link))
                continue

            print(entry.link)
            article = download_article(entry.link)

            enrich_article_with_rss_data(article, entry)
            article['meta_data'] = dict(article['meta_data'])
            article['downloaded_at'] = now
            article['feed_url'] = feed_url
            article['main_url'] = entry.link

            row = build_bq_row(article, schemas.ARTICLES_TABLE_SCHEMA, skip_fields=['html'])

            rows.append(row)
        except Exception as e:
            print('Exception while downloading/parsing feed entry {}: {}'.format(entry.link, e))
    return rows


if __name__ == '__main__':
    from .bigquery import BigqueryHelper
    from .config_utils import load_instance

    config = load_instance('/home/javier/wkf/newsgraph/instances/instance1')
    bq_helper = BigqueryHelper(config)

    # feed_url = 'https://elpais.com/tag/rss/latinoamerica/a/'
    feed_url = 'http://elmundotoday.com/rss'

    past_urls = bq_helper.list_saved_articles(feed_url)
    print('found {} urls of that feed'.format(len(past_urls)))

    articles = download_feed_articles(feed_url, skip_urls=past_urls)

    bq_helper.insert_articles(articles)
