"""
Common helpers for working with ISO8601 logfile names.
"""

import collections
import datetime
import logging
import re

#
# Globals
#

HOUR_MINUTE_PAT = re.compile(
    r'(.+)-' # prefix
    r'(\d{4})-?(\d\d)-?(\d\d)T(\d\d):?(\d\d)Z' # year, mo, day, min, hour
    r'(-[^.]+)?' # suffix, plus leading -
    r'(\..+)' # extension
    )

LogFile = collections.namedtuple(
    'LogFile',
    ('prefix', 'timestamp', 'suffix', 'extension', 'filename')
    )

SAMPLE_LOGFILE = LogFile(
    'haproxy',
    datetime.datetime(2013, 7, 27, 13, 00),
    'i-3949aea',
    '.log',
    'haproxy-20130727T1300Z-i-3949aea.log'
    )

#
# Functions
#


def logfile_keyfunc(logfile):
    """ key function for use when sorting logfiles. """
    return (
        logfile.prefix, logfile.timestamp, logfile.suffix, logfile.extension
        )


def parse_filename(filename):
    """
    Takes a log filename of the format:

        PREFIX-ISO8601[-SUFFIX].EXTENSION

    Returns a LogFile named tuple, of the format

        (prefix, iso8601, suffix, extension, filename)

    If no suffix is present, suffix will be None.
    """
    match = HOUR_MINUTE_PAT.search(filename)
    if not match:
        logging.debug('parse.parse_filename: no match for %s', filename)
        return

    prefix, year, mo, day, minute, hour, suffix, extension = match.groups()

    if suffix:
        suffix = suffix[1:]

    try:
        timestamp = datetime.datetime(*map(int, (year, mo, day, minute, hour)))
    except ValueError:
        return

    # logging.debug('parse.parse_filename: parsed %s')
    return LogFile(
        prefix,
        timestamp,
        suffix,
        extension,
        filename
        )

def unparse_filename(prefix, timestamp, suffix, extension):
    if suffix:
        suffix = '-' + suffix
    else:
        suffix = ''
    return '%s-%s%s%s' % (
        prefix, timestamp.strftime('%Y%m%dT%H%MZ'), suffix, extension)


def group_filenames(filenames):
    """
    Takes an iterable of filenames. Returns them back as a dict,
    of the format:
        {
            (prefix, suffix, extension): [ LogFiles ],
            ...
        }

    where LogFiles are sorted lexically.

    Filenames that don't parse are excluded from the dict.
    """

    result = {}
    for filename in filenames:
        lf = parse_filename(filename)
        if lf is not None:
            key = (lf.prefix, lf.suffix, lf.extension)
            result.setdefault(key, []).append(lf)
    for group in result.itervalues():
        group.sort(key=logfile_keyfunc)
    return result
