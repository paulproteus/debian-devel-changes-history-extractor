from urllib.parse import urljoin
from html.parser import HTMLParser


class DateIndexPageParser(HTMLParser):
    def error(self, message):
        pass

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.next_date_index_page = None
        self.message_urls = []
        self._in_link = False
        self._current_link_href = None
        self._current_link_text = None

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for (k, v) in attrs:
                if k == 'href':
                    self._in_link = True
                    self._current_link_href = v
                    self._current_link_text = ''

    def handle_endtag(self, tag):
        if tag == 'a':
            self._in_link = False
            if self._current_link_text == 'next page':
                self.next_date_index_page = urljoin(self.url, self._current_link_href)
            elif 'msg' in self._current_link_href:
                # There's no consistent subject line pattern to look for, so harvest everything that
                # looks like a message based on the href= containing 'msg'.
                self.message_urls.append(urljoin(self.url, self._current_link_href))
            self._current_link_href = None
            self._current_link_text = None

    def handle_data(self, data):
        if self._in_link:
            self._current_link_text += data
