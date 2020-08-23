## Status

**Archived** in favor of doing this work within UDD itself.

## Summary

Get a sqlite file with a history of package uploads in Debian.

The short-term purpose is to allow immediate social science analysis of Debian uploads.

The long-term purpose is to use this code to generate the `upload_history` series of tables in UDD (the Ultimate
Debian Database). I propose that in the UDD context, we would run this code and allow it to generate a sqlite file,
then copy the sqlite file's contents into the UDD postgres instance.

This program downloads email archives from https://lists.debian.org/debian-devel-changes/, storing them in a
cache file for performance. It parses the email sent to debian-devel-changes that accompanies every upload to Debian.
It takes care to keep the cache fresh and not re-download URLs unnecessarily.

In the future, it would be possible to adjust the code to avoid reading data from the web, e.g., reading email archives
(mbox files) directly. I personally think reading data from the web is fine; it allows the program to operate
with no special privileges.

## Status

It works splendidly for emails from 2008 onward. (Some emails from pre-2008 trigger parse failures. For information
about this and other issues, see GitHub issues.)

On my laptop, it takes 4.8 seconds to run when the cache is fresh, i.e., if no uploads have occurred since it was
previously run.

## How to use

### Run `main.py` to create `upload_history.sqlite`

Run with no arguments to extract all available data from all email archives starting with January 2008. We use
`python3.7` below since that is the Python version on Buster (current Debian release); you can use any Python >= 3.5.

```
python3.7 -m venv venv
./venv/bin/pip install aiohttp python-debian
./venv/bin/python3 main.py
```

Output is stored in `upload_history.sqlite`.

The program will exit with status 0 if it either successfully processes all messages it finds, or logs errors peacefully
while failing to process a message.

The program creates and uses a cache file at `$HOME/.cache/debian-devel-changes-history-extractor.sqlite`. HTML pages
and other temporary data are stored here. If errors are encountered processing any messages, their message IDs and full
text are stored in `~/.cache/ddc-errors.txt`. This cache file may take up to about 4.5GB of disk space in normal
operation.

You may specify a year and/or month to import by passing `-y` and `-m` respectively. This is primarily intended for
development. If you lack the disk space to cache all messages for all years, try running with `-y 2020` (for example).

### Query `upload_history.sqlite`

`upload_history.sqlite` will contain an `upload_history` table with the following columns.

- id (integer): autoincrementing numeric ID; used for foreign keys
- source (string): Package name (i.e., Debian source package name)
- version (string): Package version in string format
- date (datetime): UTC datetime of the date within the upload metadata
- changed_by_name (string): Name of person who performed this upload to Debian
- changed_by_email (string): Email address of person who performed this upload to Debian
- maintainer_name (string): Name of person/team who maintains this package (as printed in upload metadata)
- maintainer_email (string): Email address of person/team who maintains this package (as printed in upload metadata)
- message_id (string): Message ID from which this data was imported. This is assumed to be unique.

If you have `sqlite3` installed locally, you can query the file. To load the file, run this command.

```
$ sqlite3 upload_history.sqlite
SQLite version 3.28.0 2019-04-15 14:49:49
Enter ".help" for usage hints.
sqlite>
```

At the `sqlite>` prompt, you can run further queries. Here are some examples.

Get the most recent 10 uploads to Debian.

```
sqlite> select source, version, changed_by_email from upload_history ORDER BY date DESC LIMIT 10;
coderay|1.1.3-2|dai@debian.org
libmail-box-perl|3.009-1|gregoa@debian.org
libobject-pad-perl|0.32-1|gregoa@debian.org
libtest2-suite-perl|0.000135-1|gregoa@debian.org
libnet-amazon-s3-perl|0.91-1|gregoa@debian.org
wbar|2.3.4-10|apo@debian.org
libosl|0.8.0-3|dirson@debian.org
dossizola|1.0-12|dirson@debian.org
speech-dispatcher|0.10.1-2|sthibault@debian.org
mate-desktop|1.24.1-1|sunweaver@debian.org
```

Get the most recent upload by one specific maintainer.

```
sqlite> select source, version, changed_by_email from upload_history WHERE changed_by_email="gregoa@debian.org" ORDER BY date DESC LIMIT 1;
libmail-box-perl|3.009-1|gregoa@debian.org
```
