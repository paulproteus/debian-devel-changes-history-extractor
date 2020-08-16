import asyncio
import datetime
import gzip
import io
import itertools
from pathlib import Path
import re
import sqlite3

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
    cache_db = sqlite3.connect(cache_dir / "debian-devel-changes-history-extractor.sqlite")
    return cache_db.cursor()


async def main():
    # Decide if we're operating over all months, or instead specific months via argv.
    import sys
    year_index = sys.argv.index('-y')
    if year_index != -1:
        chosen_year = int(sys.argv[year_index + 1])
    else:
        chosen_year = None
    month_index = sys.argv.index('-m')
    if month_index != -1:
        chosen_month = int(sys.argv[month_index + 1])
    else:
        chosen_month = None
    # Download all HTML messages from debian-devel-changes for those months.
    cache_db = get_cache_db()
    async for (year, month, last_updated) in get_cache_stale_months(cache_db, chosen_year, chosen_month):
        if chosen_year is None or chosen_year == year:
            if chosen_month is None or chosen_month == month:
                await store_messages_in_cache(cache_db, year, month, last_updated)
    # Extract message ID & body text from each message.
    if chosen_month is None:
        chosen_months = range(1, 13)
    else:
        chosen_months = [chosen_month]
    if chosen_year is None:
        chosen_years = range(1997, 2022)
    else:
        chosen_years = [chosen_year]
    relevant_months = sorted(itertools.product(chosen_years, chosen_months))
    for (year, month) in relevant_months:
        get_message_bodies(cache_db, year, month)
    # Create output tables: `upload_history`, etc.
    output_db = sqlite3.connect("upload_history.sqlite")
    for (year, month) in relevant_months:
        get_upload_history(cache_db, output_db, year, month)
    # Close asyncio tasks in the aiohttp session.
    session = getattr(fetch, '_session', None)
    if session:
        await session.close()


def get_upload_history(cache_db, output_db, year, month):
    output_db.execute("""CREATE TABLE IF NOT EXISTS upload_history (
        message_id text PRIMARY KEY,
        source text NOT NULL,
        version text NOT NULL,
        date integer NOT NULL
    );
    """)
    gzip_content_rows = cache_db.execute(
        'SELECT message_id, body_gzip FROM message_body_and_id WHERE year=? AND month=?', (year, month)).fetchall()
    output_db.execute('BEGIN TRANSACTION;')
    for (message_id, body_gzip) in gzip_content_rows:
        body_bytes = gzip.decompress(body_gzip)
        body = body_bytes.decode('utf-8')
        date, source, version = page_parsers.metadata_from_message_body(body)
        output_db.execute(
            "INSERT OR IGNORE INTO upload_history (message_id, source, version, date) VALUES (?, ?, ?, ?)",
            (message_id, source, version, date))
    output_db.execute('COMMIT;')
    print("Computed upload history for {year}-{month:02d}".format(year=year, month=month))


def get_message_bodies(cache_db, year, month):
    cache_db.execute("""CREATE TABLE IF NOT EXISTS message_body_and_id (
        message_id text PRIMARY KEY,
        year integer NOT NULL,
        month integer NOT NULL,
        body_gzip blob NOT NULL
    );""")
    # Skip if already done
    is_done = cache_db.execute(
        'SELECT COUNT(*) FROM message_body_and_id WHERE year=? AND month=?', (year, month)).fetchone()[0]
    if is_done:
        print("Using precomputed message bodies for {year}-{month:02d}".format(year=year, month=month))
        return
    gzip_content_rows = cache_db.execute(
            'SELECT gzip_contents FROM url_contents WHERE year=? AND month=?', (year, month)).fetchall()
    cache_db.execute('BEGIN TRANSACTION;')
    for row in gzip_content_rows:
        html = gzip.decompress(row[0]).decode('utf-8', 'replace')
        parser = page_parsers.MessagePageParser()
        parser.feed(html)
        parser.close()
        cache_db.execute("""INSERT INTO message_body_and_id (
            message_id, year, month, body_gzip
        ) VALUES (?, ?, ?, ?)""", (
            parser.message_id, year, month, gzip.compress(parser.message_body.encode('utf-8'))
        ))
    cache_db.execute('COMMIT;')


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


async def store_messages_in_cache(cache_db, year, month, current_last_updated):
    print("Downloading messages for {year}-{month:02d}".format(year=year, month=month))
    cache_db.execute("BEGIN TRANSACTION;")
    await download_message_urls_for_month(cache_db, year, month)
    update_cached_month_index_last_updated(cache_db, year, month, current_last_updated)
    cache_db.execute("COMMIT;")


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


def update_cached_month_index_last_updated(cache_db, year, month, current_last_updated):
    cache_db.execute(
        "INSERT INTO month_index_last_updated (year, month, last_updated) VALUES (?, ?, ?)",
        (year, month, current_last_updated)
    )


async def store_url(cache_db, year, month, url):
    cache_db.execute("""
    CREATE TABLE IF NOT EXISTS url_contents (
        url string PRIMARY KEY,
        year integer NOT NULL,
        month integer NOT NULL,
        gzip_contents blob
    );
    """)
    contents = await fetch(url)
    gzip_contents = io.BytesIO()
    gzip_fd = gzip.GzipFile(fileobj=gzip_contents, mode='wb')
    gzip_fd.write(contents)
    gzip_fd.close()
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
