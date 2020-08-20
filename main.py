import asyncio
import datetime
import gzip
import io
import itertools
from pathlib import Path
import re
import sqlite3
import sys
import traceback

import aiohttp

import page_parsers


async def fetch(url):
    session = getattr(fetch, '_session', None)
    if session is None:
        setattr(fetch, '_session', aiohttp.ClientSession(headers={"Connection": "keep-alive"}))
        session = getattr(fetch, '_session')
    try:
        async with session.get(url) as response:
            return await response.read()
    except aiohttp.client_exceptions.ServerDisconnectedError:
        # Wait 1 sec, then retry once
        await asyncio.sleep(1)
        async with session.get(url) as response:
            return await response.read()


def get_cache_db():
    # Each month requires approx 25MB of cache storage.
    cache_dir = Path.home() / '.cache'
    cache_dir.mkdir(mode=0o700, exist_ok=True)
    return sqlite3.connect(cache_dir / "debian-devel-changes-history-extractor.sqlite")


async def main():
    # Decide if we're operating over all months, or instead specific months via argv.
    import sys
    if '-y' in sys.argv:
        year_index = sys.argv.index('-y')
        chosen_year = int(sys.argv[year_index + 1])
    else:
        chosen_year = None
    if '-m' in sys.argv:
        month_index = sys.argv.index('-m')
        chosen_month = int(sys.argv[month_index + 1])
    else:
        chosen_month = None
    # Download all HTML messages from debian-devel-changes for those months.
    cache_db = get_cache_db()
    async for (year, month, last_updated) in get_cache_stale_months(cache_db.cursor(), chosen_year, chosen_month):
        if chosen_year is None or chosen_year == year:
            if chosen_month is None or chosen_month == month:
                await store_messages_in_cache(cache_db.cursor(), year, month, last_updated)
    # Extract message ID & body text from each message.
    if chosen_month is None:
        chosen_months = range(1, 13)
    else:
        chosen_months = [chosen_month]
    if chosen_year is None:
        # Starting at 2008, because earlier years have some parsing problems; would be nice to fix them.
        chosen_years = range(2008, 2022)
    else:
        chosen_years = [chosen_year]
    relevant_months = sorted(itertools.product(chosen_years, chosen_months))
    for (year, month) in relevant_months:
        get_message_bodies(cache_db, year, month)
    # Create output tables: `upload_history`, etc.
    output_db = sqlite3.connect("upload_history.sqlite")
    for (year, month) in relevant_months:
        get_upload_history(cache_db.cursor(), output_db, year, month)
    # Close asyncio tasks in the aiohttp session.
    session = getattr(fetch, '_session', None)
    if session:
        await session.close()


def _in_query(query_before_in, items):
    # Helper for using the sqlite3 `IN` operator, which requires creating a lot of
    # question marks.
    return query_before_in + 'IN ( ' + (
        ','.join("?" * len(items)) +
        " )"
    )


BEGIN_PGP_REGEX = re.compile(rb'^ *-+ *BEGIN PGP', re.MULTILINE)


def get_upload_history(cache_db, output_db, year, month):
    output_db.execute("""CREATE TABLE IF NOT EXISTS upload_history (
        message_id text PRIMARY KEY,
        date integer NOT NULL,
        source text NOT NULL,
        version text NOT NULL,
        changed_by text,
        changed_by_name text,
        changed_by_email text,
        maintainer text NOT NULL,
        maintainer_name text NOT NULL,
        maintainer_email NOT NULL,
        nmu boolean NOT NULL,
        changes text NOT NULL
    );
    """)
    # Notes about the table and its columns.
    #
    # The `changed_by` series of columns (`changed_by`, `changed_by_name`, `changed_by_email`)
    # comes from a `Changed-by:` header in the upload metadata. It is present for recent messages
    # e.g. https://lists.debian.org/debian-devel-changes/2020/08/msg00003.html but not old messages
    # e.g. https://lists.debian.org/debian-devel-changes/1997/08/msg00000.html .
    this_month_msg_ids = list(map(
        lambda row: row[0], cache_db.execute(
            'SELECT message_id FROM message_body_and_id WHERE year=? AND month=?', (year, month))
    ))
    already_processed_msg_ids = set(
        map(
            lambda row: row[0],
            output_db.execute(
                _in_query('SELECT message_id FROM upload_history WHERE message_id ', this_month_msg_ids),
                this_month_msg_ids)
        )
    )
    unprocessed_message_ids = [
        msg_id for msg_id in this_month_msg_ids
        if msg_id not in already_processed_msg_ids
    ]
    gzip_content_rows = cache_db.execute(
        _in_query('SELECT message_id, body_gzip FROM message_body_and_id WHERE message_id ', unprocessed_message_ids),
        unprocessed_message_ids
    )

    output_db.execute("BEGIN TRANSACTION;")
    for (message_id, body_gzip) in gzip_content_rows:
        body = gzip.decompress(body_gzip)

        # Attempt to parse the message three times. First, try parsing the whole message.
        # This is important for early (1997-era) messages which lack
        # Try parsing it as-is.
        try:
            full_message_parsed_result = page_parsers.metadata_from_message_body(body)
        except Exception:
            full_message_parsed_result = None

        if False and full_message_parsed_result is None:
            # Seek forward to the start of the "-----BEGIN PGP SIGNED MESSAGE-----", or
            # skip the message_id if there is none.
            pgp_regex_result = BEGIN_PGP_REGEX.search(body)
            if pgp_regex_result is None:
                error_fd = (Path.home() / '.cache' / 'ddc-errors.txt').open('ab')
                error_text = f'msg_id has no content? {message_id}\n'.format(message_id=message_id)
                print(error_text, file=sys.stderr)
                error_fd.write(error_text.encode('utf-8'))
                error_fd.write(body)
                error_fd.write(b'\n')
                print("see also ~/.cache/ddc-errors.txt", file=sys.stderr)
                error_fd.close()
                continue

            # Slice body so it begins at the PGP metadata.
            sliced_body = body[pgp_regex_result.span()[0]:]
        else:
            sliced_body = body

        # If there's no 'Source: ' line, then it's not an upload, so skip it.
        if b'\nSource: ' not in sliced_body:
            print("Skipping message_id because no source package line can be found " + message_id)
            continue

        try:
            results = page_parsers.metadata_from_message_body(sliced_body)
        except Exception:
            error_fd = (Path.home() / '.cache' / 'ddc-errors.txt').open('ab')
            error_text = f'msg_id fail {message_id}\n'.format(message_id=message_id)
            error_text += traceback.format_exc()
            print(error_text, file=sys.stderr)
            error_fd.write(error_text.encode('utf-8'))
            error_fd.write(sliced_body)
            error_fd.write(b'\n')
            print("see also ~/.cache/ddc-errors.txt", file=sys.stderr)
            error_fd.close()
            continue

        if results is None:
            print("Skipping message_id because no date line can be found " + message_id)
            continue

        metadata = (message_id, *results)
        output_db.execute("""
            INSERT INTO upload_history (
            message_id,
            date, source, version,
            changed_by, changed_by_name, changed_by_email,
            maintainer, maintainer_name, maintainer_email,
            nmu, changes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, metadata)
    output_db.execute('COMMIT;')
    print("Computed upload history for {year}-{month:02d}".format(year=year, month=month))


def get_message_bodies(cache_db, year, month):
    cursor = cache_db.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS message_body_and_id (
        message_id text PRIMARY KEY,
        year integer NOT NULL,
        month integer NOT NULL,
        body_gzip blob NOT NULL
    );""")
    cursor.execute(
        """CREATE INDEX IF NOT EXISTS message_body_and_id__year_month_index
        ON message_body_and_id (year, month);""")
    # Skip if already done
    one_message_id_imported_this_month = cursor.execute(
        'SELECT message_id FROM message_body_and_id WHERE year=? AND month=? LIMIT 1', (year, month)).fetchone()
    if one_message_id_imported_this_month:
        print("Using precomputed message bodies for {year}-{month:02d}".format(year=year, month=month))
        return
    gzip_content_rows = cache_db.cursor().execute(
        'SELECT gzip_contents FROM url_contents WHERE year=? AND month=?', (year, month))
    cursor.execute('BEGIN TRANSACTION;')
    for row in gzip_content_rows:
        html = gzip.decompress(row[0]).decode('utf-8', 'replace')
        parser = page_parsers.MessagePageParser()
        parser.feed(html)
        parser.close()
        body_gzip = gzip.compress(parser.message_body.encode('utf-8'))
        if parser.message_body is None:
            import pdb; pdb.set_trace()
            parser.message_body = ''
        cache_db.execute("""INSERT INTO message_body_and_id (
            message_id, year, month, body_gzip
        ) VALUES (?, ?, ?, ?)""", (
            parser.message_id, year, month, body_gzip,
        ))
    cursor.execute('COMMIT;')


async def month_index_last_updated(year, month):
    """Visit a URL like https://lists.debian.org/debian-devel-changes/2020/06/maillist.html
    to get the current "The last update was on..." message."""
    url = "https://lists.debian.org/debian-devel-changes/{year:d}/{month:02d}/maillist.html".format(
        year=year, month=month,
    )
    month_index_bytes = await fetch(url)
    return re.search('The last update was on [^.]*[.]', month_index_bytes.decode('ascii', 'replace')).group()


async def get_cache_stale_months(cache_db, chosen_year, chosen_month):
    all_months = [(1997, 8), (1997, 9), (1997, 10), (1997, 11), (1997, 12)]
    tasks = []
    today_month = datetime.date.today().month
    today_year = datetime.date.today().year
    for year in range(1998, 2021):
        for month in range(1, 13):
            if year == today_year and month > today_month:
                break
            all_months.append((year, month))
    check_these_months = [
        (year, month) for (year, month) in all_months
        if (chosen_year is None or year == chosen_year) and (chosen_month is None or month == chosen_month)
    ]
    for (year, month) in check_these_months:
        tasks.append(get_cache_freshness(cache_db, year, month))
    results = await asyncio.gather(*tasks)
    for i, last_updated_text in enumerate(results):
        if last_updated_text is None:
            continue
        year, month = check_these_months[i]
        yield year, month, last_updated_text


async def get_cache_freshness(cache_db, year, month):
    print("Checking cache freshness for {year}-{month:02d}".format(year=year, month=month))
    current_last_updated = await month_index_last_updated(year, month)
    month_fresh = (
            current_last_updated ==
            cached_month_index_last_updated(cache_db, year, month)
    )
    if month_fresh:
        return None
    return current_last_updated


async def store_messages_in_cache(cache_db_cursor, year, month, current_last_updated):
    print("Downloading messages for {year}-{month:02d}".format(year=year, month=month))
    cache_db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS url_contents (
        url string PRIMARY KEY,
        year integer NOT NULL,
        month integer NOT NULL,
        gzip_contents blob
    );
    """)
    cache_db_cursor.execute("""
        CREATE INDEX IF NOT EXISTS url_contents__year_month_index
        ON url_contents (year, month);""")
    cache_db_cursor.execute("BEGIN TRANSACTION;")
    await download_message_urls_for_month(cache_db_cursor, year, month)
    cache_db_cursor.execute(
        "DELETE FROM month_index_last_updated WHERE year=? AND month=?", (year, month))
    cache_db_cursor.execute(
        "INSERT INTO month_index_last_updated (year, month, last_updated) VALUES (?, ?, ?)",
        (year, month, current_last_updated)
    )
    cache_db_cursor.execute("COMMIT;")
    cache_db_cursor.close()


async def download_message_urls_for_month(cache_db, year, month):
    """Get all message URLs for the given month, but do not download them yet"""
    next_date_index_page = "https://lists.debian.org/debian-devel-changes/{year:d}/{month:02d}/maillist.html".format(
        year=year, month=month,
    )
    message_urls = []
    while next_date_index_page is not None:
        print("...getting messages from " + next_date_index_page)
        parsed_date_index_page = page_parsers.DateIndexPageParser(
            next_date_index_page)
        parsed_date_index_page.feed((await fetch(next_date_index_page)).decode('utf-8', 'replace'))
        parsed_date_index_page.close()
        next_date_index_page = parsed_date_index_page.next_date_index_page
        message_urls.extend(parsed_date_index_page.message_urls)
    # Run all tasks, allowing exceptions to bubble up, without leaking any tasks.
    all_tasks = asyncio.gather(
        *[store_url(cache_db, year, month, message_url) for message_url in message_urls]
    )
    try:
        await all_tasks
    finally:
        # Cancel any tasks that didn't happen
        all_tasks.cancel()


async def store_url(cache_db, year, month, url):
    contents = await fetch(url)
    gzip_contents = io.BytesIO()
    gzip_fd = gzip.GzipFile(fileobj=gzip_contents, mode='wb')
    gzip_fd.write(contents)
    gzip_fd.close()
    cache_db.execute("DELETE FROM url_contents WHERE url=? AND year=? AND month=?", (url, year, month))
    cache_db.execute(
        "INSERT INTO url_contents (url, year, month, gzip_contents) VALUES (?, ?, ?, ?)",
        (url, year, month, gzip_contents.getvalue())
    )


def cached_month_index_last_updated(cache_db, year, month):
    """Return the most recently stored month_index_last_updated value for the given year & month, or None."""
    cache_db.execute("""
    CREATE TABLE IF NOT EXISTS month_index_last_updated (
        year integer,
        month integer,
        last_updated text,
        PRIMARY KEY (year, month)
    );
    """)
    rows = cache_db.execute(
        "SELECT last_updated FROM month_index_last_updated WHERE year = ? AND month = ? LIMIT 1",
        (year, month)
    ).fetchall()
    if rows:
        return rows[0][0]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
