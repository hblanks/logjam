"""
Uploader
"""

import argparse
import logging
import os
import os.path
import sys
import urlparse

from . import parse
from . import service


COMMAND_DESCRIPTION = \
"""Takes an archive/ directory of compressed ISO8601 logfiles and a URL
to upload them to. Uploads the files to that URL.

Sample usage:

    logjam-upload /var/log/hourly/archive/ \
	s3://my-log-bucket/{prefix}/{year}/{month}/{day}/{filename}

Eventually, this tool should also allow operators to automatically delete
older logfiles that have already been uploaded.

"""


#
# Import helpers
#

def boto_exists():
	try:
		import boto
	except ImportError:
		return False
	return True



#
# The dict of uploaders
#

UPLOADERS = {}

if boto_exists():
	from . import s3_uploader
	UPLOADERS['s3'] = s3_uploader.S3Uploader
else:
	logging.info('upload: failed to import boto. S3Uploader will not be available')


def get_uploader(upload_uri, uploaders=UPLOADERS):
    u = urlparse.urlparse(upload_uri)
    if u.scheme not in uploaders:
        raise Exception(
            'No uploader found for URI scheme {}'.format(u.scheme)
            )
    return uploaders[u.scheme](upload_uri)


#
# Core functions
#

def scan_and_upload_filenames(log_archive_dir, filenames, uploader):
    logfiles = filter(None, map(parse.parse_filename, filenames))

    uploaded, not_uploaded = uploader.scan_remote(logfiles)
    # logging.debug('scan_and_upload: uploaded %r',
    #     [l.filename for l in sorted(uploaded)]
    #     )
    # logging.debug('scan_and_upload: not_uploaded %r',
    #     [l.filename for l in sorted(not_uploaded)]
    #     )

    # Make a fresh, sorted list as we'll be mutating it.
    for logfile in sorted(not_uploaded):
        error = uploader.upload_logfile(log_archive_dir, logfile)
        if error:
            logging.warning('scan_and_upload: failed to upload %s',
                logfile.filename
                )
        else:
            logging.info('scan_and_upload: uploaded %s',
                logfile.filename
                )
            not_uploaded.remove(logfile)
            uploaded.add(logfile)

    return uploaded, not_uploaded


def scan_and_upload(log_archive_dir, log_upload_uri, persist_hours=None):
    """
    Takes a directory of archived logfiles, an upload URI, and an
    optional number of how many hours of old logfiles to keep.

    Scans the directory of archived logfiles and compares it with those
    at the upload URI. Uploads files that are not present, and then
    prunes logfiles that are more than persist_hours hours old.
    """

    filenames = os.listdir(log_archive_dir)
    uploader = get_uploader(log_upload_uri)

    uploader.connect()
    error = uploader.check_uri()
    if error:
        logging.error('Invalid upload_uri %s: %s', log_upload_uri, error)
        print >> sys.stderr, 'Invalid upload_uri %s: %s' % (
            log_upload_uri, error
            )
        sys.exit(1) # take the short way out

    uploaded, not_uploaded = scan_and_upload_filenames(
        log_archive_dir, filenames, uploader
        )

    if persist_hours:
        # TODO: prune old logfiles here using uploaded_set
        raise NotImplementedError


#
# CLI functions
#


def make_parser():
    parser = argparse.ArgumentParser(
        description=COMMAND_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter
        )
    parser.add_argument(
        'log_archive_dir',
        help='Directory in which to scan for, and compress, hourly log files',
        )
    parser.add_argument(
        'log_upload_uri',
        help='Upload URI. Must contain {prefix}, {year}, {month}, {day}, and {filename}.'
        )
    parser.add_argument(
        '--once',
        action='store_true',
        help=(
            'Scan and compress directory once, then exit, instead of running'
            'continuously.'
            )
        )
    parser.add_argument(
        '--log-level', '-l',
        choices=('debug', 'info', 'warning', 'error', 'critical'),
        default='info',
        help='Log level to use for logjam\'s own logging',
        )
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv[1:])

    if not os.path.basename(args.log_archive_dir) == 'archive':
        parser.error(
            'log_archive_dir {!r} does not end in "/archive'.format(
                args.log_archive_dir
                )
            )

    service.configure_logging(args.log_level)

    # Tune down boto logging
    logging.getLogger('boto').setLevel(logging.WARNING)

    if args.once:
        scan_and_upload(
            args.log_archive_dir,
            args.log_upload_uri,
            )
    else:
        service.do_forever(
            scan_and_upload,
            service.DEFAULT_INTERVAL,
            args.log_archive_dir, args.log_upload_uri
            )
