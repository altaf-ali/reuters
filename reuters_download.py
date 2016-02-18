import os
import sys
import time

import datetime
import dateutil.parser

import bs4
import urllib2
import urlparse

import html2text

class MetaTags(object):
    def __init__(self, soup):
        self.tags = {}

        for tag in soup.find_all('meta'):
            name = tag.get('name')
            if name != None:
                self.tags[name] = tag.get('content')
            name = tag.get('property')
            if name != None:
                self.tags[name] = tag.get('content')

    def get(self, name):
        if self.tags.has_key(name):
            return self.tags[name]
        return None

class ReutersContent(object):
    def __init__(self, html):
        self.id = None
        self.type = None
        self.section = None
        self.timestamp = None
        self.location = 'Unknown'
        self.html = html
        self.text = None

    def parse(self):
        soup = bs4.BeautifulSoup(self.html)
        tags = MetaTags(soup)

        self.id = tags.get('DCSext.ContentID')
        self.type = tags.get('DCSext.ContentType')
        self.section = tags.get('DCSext.ContentChannel')
        self.timestamp = dateutil.parser.parse(tags.get('REVISION_DATE'))

        div = soup.find('div', id='articleInfo')
        span = div.find('span', class_='location')
        if span != None:
            self.location = span.text

        span = soup.find('span', id='articleText')
        if span != None:
            html = html2text.HTML2Text()
            html.ignore_links = True

            self.text = html.handle(span.prettify())

class ReutersCorpus(object):
    def __init__(self):
        self.root = 'http://www.reuters.com'
        self.url_timeout = 30
        self.retry_delay = 2
        self.corpus_start = datetime.date(2006, 10, 26)

    def make_url(self, url):
        return urlparse.urljoin(self.root, url)

    def get_week_offset(self, date):
        return ((date - self.corpus_start).days / 7) + 1

    def write_doc(self, output_dir, date, id, text):
        week=self.get_week_offset(date)
        filename='%04d_%04d-%02d-%02d_%s.txt' % (week, date.year, date.month, date.day, id)
        with open(os.path.join(output_dir, filename), 'wb') as output_file:
            output_file.write(text)

    def download_doc(self, output_dir, date, url, title):
        print '%s: %s - %s' % (date, title, url)

        #if file exists:
        #    return False

        try:
            doc = urllib2.urlopen(url, timeout=self.url_timeout)

        except:
            print "unhandled exception:%s" % (sys.exc_info()[0])
            return False

        else:
            html = doc.read()

            content = ReutersContent(html)
            content.parse()

            self.write_doc(output_dir, date, content.id, content.text)

            return True

    def download(self, output_dir, start_date, end_date):
        url_format = 'news/archive/worldnews?date=%02d%02d%4d'

        date = start_date
        while (date <= end_date):
            url = url_format % (date.month, date.day, date.year)

            try:
                print 'loading', self.make_url(url)
                doc = urllib2.urlopen(self.make_url(url), timeout=self.url_timeout)

            except:
                print "unhandled exception:%s" % (sys.exc_info()[0])
                time.sleep(self.retry_delay)
                continue

            try:
                soup = bs4.BeautifulSoup(doc)
                div = soup.find('div', class_='moduleBody')
                for feature in div.find_all('div', class_='feature'):
                    a = feature.find('a')
                    result = self.download_doc(
                        output_dir=output_dir,
                        date = date,
                        url = self.make_url(a.get('href')),
                        title = a.get_text(),
                    )

            except:
                print "unhandled exception:%s" % (sys.exc_info()[0])

            date += datetime.timedelta(days=1)

        print 'download finished'


def main():
    corpus = ReutersCorpus()

    output_dir = '/Users/altaf/Projects/reuters/source/test'
    start_date = datetime.date(2009, 1, 1)
    end_date = datetime.date(2009, 1, 2)

    corpus.download(output_dir, start_date, end_date)

if __name__ == "__main__":
    main()
