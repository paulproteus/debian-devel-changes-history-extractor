import asyncio
import datetime
import gzip
import io
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
    cache_dir = Path.home() / '.cache'
    cache_dir.mkdir(mode=0o700, exist_ok=True)
    cache_db = sqlite3.connect(cache_dir / "debian-devel-changes-history-extractor.sqlite")
    return cache_db.cursor()


async def main():
    cache_db = get_cache_db()
    async for (year, month, last_updated) in get_cache_stale_months(cache_db):
        await store_messages_in_cache(cache_db, year, month, last_updated)
    print('want to compute_upload_history()')
    session = getattr(fetch, '_session', None)
    if session:
        await session.close()


async def month_index_last_updated(year, month):
    """Visit a URL like https://lists.debian.org/debian-devel-changes/2020/06/maillist.html
    to get the current "The last update was on..." message."""
    url = "https://lists.debian.org/debian-devel-changes/{year:d}/{month:02d}/maillist.html".format(
        year=year, month=month,
    )
    month_index_bytes = await fetch(url)
    return re.search('The last update was on [^.]*[.]', month_index_bytes.decode('ascii', 'replace')).group()


async def get_cache_stale_months(cache_db):
    months = [(1997, 8), (1997, 9), (1997, 10), (1997, 11), (1997, 12)]
    tasks = []
    today_month = datetime.date.today().month
    today_year = datetime.date.today().year
    for year in range(1998, 2021):
        for month in range(1, 13):
            if year == today_year and month > today_month:
                break
            months.append((year, month))
            tasks.append(get_cache_freshness(cache_db, year, month))
    results = await asyncio.gather(*tasks)
    for i, last_updated_text in enumerate(results):
        if last_updated_text is None:
            continue
        year, month = months[i]
        yield (year, month, last_updated_text)


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
        year integer,
        month integer,
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
