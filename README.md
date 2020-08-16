## Summary

Get a sqlite file with a history of package uploads in Debian.

## How to use

Run with no arguments to extract all available data from all historic email
archives.

```
python3 main.py
```

Output is stored in `upload_history.sqlite`; override with `-o path`.

A cache file at `$HOME/.cache/debian-devel-changes-history-extractor.sqlite` is
used. HTML pages and other temporary data are stored here. If errors are encountered processing any
messages, their message IDs are stored in the cache directory under `msg-ids-errors.txt`.

You may specify one year and month with `-y` and `-m` respectively. This is
intended for development. If you lack enough disk space to cache all messages
for all years, try running with `-y 2020` (for example).

The program will exit with status 0 if it successfully processes all messages.

## Details

This program downloads email list archives and extracts package upload history
from them. It relies on the fact that every time a new package version is
uploaded to Debian, an automated email is sent to the debian-devel-changes
list with machine-readable metadata. 

It downloads all needed HTML data into the cache before processing.

`upload_history.sqlite` will contain an `upload_history` table with the following columns:

- id: int: autoincrementing numeric ID; used for foreign keys
- source: string: Package name (i.e., Debian source package name)
- version: string: Package version in string format
- date: int: Epoch timestamp of the date within the upload metadata
- changed_by_name: string: Name of person who performed this upload to Debian
- changed_by_email: string: Email address of person who performed this upload to Debian
- maintainer_name: string: Name of person/team who maintains this package (as printed in upload metadata)
- maintainer_email: string: Email address of person/team who maintains this package (as printed in upload metadata)
- message_id: string: Message ID from which this data was imported. This is assumed to be unique.

`upload_history.sqlite` will also contain a `upload_history_closes` table with the following columns:

- id: int: autoincrementing ID
- upload_history_id: ID of a `upload_history` row
- bug: int: numeric ID of a bugs.debian.org bug
