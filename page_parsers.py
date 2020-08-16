import email.utils
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


class MessagePageParser(HTMLParser):
    def error(self, message):
        pass

    def __init__(self):
        super().__init__()
        self.message_id = None
        self.message_body = None
        self._next_li_body_is_message_id = False
        self._in_li = False
        self._current_text = None

    def handle_starttag(self, tag, attrs):
        if tag == 'li' or tag == 'em' or tag == 'pre':
            self._current_text = ''
        if tag == 'li':
            self._in_li = True

    def handle_data(self, data):
        if self._current_text is not None:
            self._current_text += data

    def handle_endtag(self, tag):
        if tag == 'pre':
            self.message_body = self._current_text
            self._current_text = None
            return

        if tag == 'em':
            if self._in_li and self._current_text.lower().strip() == 'message-id':
                self._next_li_body_is_message_id = True
            self._current_text = ''
            return

        if tag == 'li':
            if self._next_li_body_is_message_id:
                # This has a format like:
                # ': <[🔎]\xa0Pine.LNX.3.96.example@example.com>'
                # Remove the <[🔎]\xa0 & the colon.
                messy_message_id = self._current_text
                if messy_message_id.startswith(': '):
                    messy_message_id = messy_message_id[2:]
                messy_message_id = messy_message_id.replace('<[🔎]\xa0', '')
                messy_message_id = messy_message_id.strip()
                if messy_message_id[0] == '<':
                    messy_message_id = messy_message_id[1:]
                if messy_message_id[-1] == '>':
                    messy_message_id = messy_message_id[:-1]
                self.message_id = messy_message_id
            self._current_text = None
            self._in_li = False
            self._next_li_body_is_message_id = False
            return


def metadata_from_message_body(body):
    # Manually parse for speed; could rely on deb822 if this does not work consistently.
    date = None
    date_string = None
    source = None
    version = None
    for line in body.split('\n'):
        if not line:
            continue
        if not line[0] in ('S', 's', 'V', 'v', 'D', 'd'):
            continue
        before_colon = line[:9].lower()
        if before_colon.startswith('source:'):
            source = line.split(':', 1)[1].strip()
        elif before_colon.startswith('version:'):
            version = line.split(':', 1)[1].strip()
        elif before_colon.startswith('date:'):
            date_string = line.split(':', 1)[1].strip()
    date = email.utils.parsedate_to_datetime(date_string).timestamp()
    return date, source, version
