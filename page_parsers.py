import datetime

import debian.deb822
import email.headerregistry
import email.utils
import html
import html.parser
import re
from urllib.parse import urljoin


class DateIndexPageParser(html.parser.HTMLParser):
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


class MessagePageParser(html.parser.HTMLParser):
    def error(self, message):
        pass

    def __init__(self):
        super().__init__()
        self.message_id = None
        self.message_body = None
        self._message_body_buffer = None
        self._next_li_body_is_message_id = False
        self._in_li = False
        self._current_text = None

    def handle_comment(self, data):
        # Rely on MHonArc's template to tell us when the body starts & ends.
        # Earlier versions of this code relied on <pre> indicating the start of body, but alas,
        # some messages have no text/plain part and therefore no <pre>.
        if data.strip() == 'X-Body-of-Message':
            self._message_body_buffer = []
        if data.strip() == 'X-Body-of-Message-End':
            self.message_body = ''.join(self._message_body_buffer)
            self._message_body_buffer = None

        # Rely on MHonArc's template to tell us the Message ID.
        if data.strip().startswith('X-Message-Id:'):
            raw_data = data[len('X-Message-Id:'):].strip()
            self.message_id = html.unescape(raw_data)

    def handle_data(self, data):
        if self._message_body_buffer is not None:
            self._message_body_buffer.append(data)


nmu_version_re = re.compile(r"(-\S+\.\d+|\+nmu\d+)$")
nmu_changelog_re = re.compile(r"\s+\*\s+.*(NMU|non[- ]maintainer)", re.IGNORECASE + re.MULTILINE)


def metadata_from_message_body(body):
    body_parsed = dict(debian.deb822.Deb822(body))
    source = body_parsed['Source']
    # Some uploads in 1997 lack a Date header. Ignore them.
    date_string = body_parsed.get('Date')
    if date_string is None:
        return None
    version = body_parsed['Version']
    changed_by = body_parsed.get('Changed-By')
    maintainer = body_parsed['Maintainer']
    changes = body_parsed['Changes']

    # Convert the date to UTC. This makes accurate querying easier: sqlite3 ignores timezones when querying.
    date = datetime.datetime.utcfromtimestamp(_parse_date_string(date_string).timestamp())

    # Use AddressHeader not SingleAddressHeader because of headers like
    # Maintainer: Foo, Bar <example@example.com>
    # which break SingleAddressHeader (there are "two addresses.")
    #
    # Also replace '\n' with ' ' so Python can parse it.
    parsed_maintainer = email.headerregistry.HeaderRegistry(
        default_class=email.headerregistry.AddressHeader, use_default_map=False
    )('maintainer', maintainer.replace('\n', ' '))
    maintainer_email = parsed_maintainer.addresses[-1].username + '@' + parsed_maintainer.addresses[-1].domain
    maintainer_name = parsed_maintainer.addresses[-1].display_name

    if changed_by is None:
        changed_by_email = None
        changed_by_name = None
    else:
        # Use AddressHeader not SingleAddressHeader for similar reasons as the Maintainer header above.
        parsed_changed_by = email.headerregistry.HeaderRegistry(
            default_class=email.headerregistry.AddressHeader, use_default_map=False
        )('changed-by', changed_by)
        changed_by_email = parsed_changed_by.addresses[-1].username + '@' + parsed_changed_by.addresses[-1].domain
        changed_by_name = parsed_changed_by.addresses[-1].display_name

    # We use a version number match regular expression, which gives false positives for example
    # with -x+y.z.w, along with checking a regexp against changes.
    # This might change in the future, see DEP1 http://wiki.debian.org/NmuDep
    nmu = bool(nmu_version_re.search(version) and nmu_changelog_re.search(changes))

    return (
        date, source, version,
        changed_by, changed_by_name, changed_by_email,
        maintainer, maintainer_name, maintainer_email,
        nmu,
    )


# Some messages have invalid time data that Python rejects.
BAD_TIME_DATA = {
    # Invalid time zones
    " +5300": "", " +8000": "", " -5000": "", " -4000": "", " +3000": "",
    # Invalid hour
    " 24:": " 23: ", " 26:": " 23:", " 85:": " 00:", " 33:": " 00:", " 29:": " 00:",
    # Invalid year
    ' 97 ': ' 1997 ',
    # Invalid time (formatting or out-of-bounds hours/seconds) (and an actual leap second)
    "08.30:43": "08:30:43", "18:63:32": "18:00:32", "00:59:60": "00:01:00",
    # Invalid seconds
    ":85 ": ":00 ", ":82 ": ":00 ",
    # Invalid day
    " 29 Feb 1999 ": " 28 Feb 1999 ",  "0 Apr 2000": "1 Apr 2000", "32 May 2004": "1 May 2004",
    # Invalid month
    " Sept ": " Sep ", " Augh ": " Aug ",
    # Valid but French month
    "Fev 2004": "Feb 2004", " Mars ": " Mar ", " Mai ": " May ", "30 Jui 2002": "30 Jul 2002",
    # April & December, in some language??
    " 26 Apt 1998 ": " 26 Apr 1998 ", " Dev 1998": " Dec 1998", " Deb 1999": " Dec 1999",
    # Valid but German month?
    " Okt 1998": " Oct 1998", " Dez ": " Dec ", " Jen 1999 ": " Jun 1999 ", " Auf 2000 ": " Aug 2000 ",
    " Dic 2002": " Dec 2002", " Dic 2003": " Dec 2003", " Deb 2004": " Dec 2004",
    " Okt 2003": " Oct 2003", "Seb 2005": "Sep 2005", "Set 2005": "Sep 2005", "Aut 2007": "Aug 2007",
    "Augl 2007": "Aug 2007",
    # Invalid day
    "Thur, ": "Thu, ",
    # Manually-written transposed month & day?
    "May, 21 Sun 2010 ": "Sun, May 21 2010 ", "May, 9 Sun 2010 ": "Sun, May 9 2010 ",
    "Sep, 23 Thu 2004": "Thu, 23 Sep 2004", "May, 19 Sun 2010": "Sun, 19 May 2010",
    # Valid but truncated month?
    " Februar 2001 ": " Feb 2001 ", "Sat, 25 My 2002": "Sat, 25 May 2002", " My 2002": " May 2002",
    "Agu 2003": "Aug 2003", "Ago 2003": "Aug 2003", "Au 2006": "Aug 2006",
    # Manually-written month and/or day?
    " 14 Sat 2002 ": " 14 Sep 2002 ", "19 Now 2002": "19 Nov 2002", "5 Agu 2003": "5 Aug 2003",
    "05 Nove 2003": "05 Nov 2003", "Marc 2004": "Mar 2004", "10 Juk 2004": "10 Jul 2004",
    "05 Decp 2004": "05 Dec 2004", "22 Wed 2002": "22 May 2002",
    "Sun,  15 Jen 2005": "Sun, 15 Jan 2005", "30 Sun 2005": "30 Jan 2005", "31 Sun 2005": "31 Jan 2005",
}

def _parse_date_string(date_string):
    try:
        return email.utils.parsedate_to_datetime(date_string)
    except Exception:
        for bad_string in BAD_TIME_DATA:
            date_string = date_string.replace(bad_string, BAD_TIME_DATA[bad_string])
        return email.utils.parsedate_to_datetime(date_string)
