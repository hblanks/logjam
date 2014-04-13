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
    """
    Args:

        - log_archive_dir: path to a directory of archived logfiles
        - filenames: filenames to (possibly) upload within this dir
        - uploader: Uploader instance

Returns:

        - set of LogFiles that are have been uploaded (now, or earlier)
        - set of LogFiles that have not yet been uploaded
    """
    logfiles = filter(None, map(parse.parse_filename, filenames))
    uploaded, not_uploaded = uploader.scan_remote(logfiles)

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


class UploadService(object):

    # Maximum size of this set: 400000 * 48 bytes per name ~= 18MB
    # 400000 hourly logfiles ~= 20 log prefixes * 24 hours * 833 days
    MAX_UPLOADED_FILENAMES = 400000

    def __init__(self, log_archive_dir, log_upload_uri, ):
        """
        Args:

            - log_archive_dir: path to a directory of archived logfiles,
            - log_upload_uri: an upload URI,

        """

        self.log_archive_dir = log_archive_dir
        self.log_upload_uri = log_upload_uri
        self.uploaded_filenames = set()


    def update_uploaded_filenames(self, uploaded_logfiles):
        """
        Updates self.uploaded_filenames, checking to make sure that we
        don't exceed our maximum set size.

        Args:

            - uploaded_logfiles: set of LogFiles that have been uploaded

        """
        if len(self.uploaded_filenames) > self.MAX_UPLOADED_FILENAMES:
            self.uploaded_logfiles = set()
            # raise RuntimeError(
            #     'Number of uploaded files tracked exceeds %d' %
            #     self.MAX_UPLOADED_FILENAMES
            # )
        for logfile in uploaded_logfiles:
            self.uploaded_filenames.add(logfile.filename)


    def run(self):
        """
        Scans the directory of archived logfiles and compares it with those
        at the upload URI. Uploads files that are not present, and then
        prunes logfiles that are more than persist_hours hours old.
        """

        uploader = get_uploader(self.log_upload_uri)
        uploader.connect()
        error = uploader.check_uri()
        if error:
            logging.error(
                'Invalid upload_uri %s: %s', self.log_upload_uri, error
            )
            raise Exception('Invalid upload_uri %s: %s' % (
                log_upload_uri, error
            ))

        filenames = os.listdir(self.log_archive_dir)
        filenames = set(filenames) - set(self.uploaded_filenames)

        uploaded, not_uploaded = scan_and_upload_filenames(
            self.log_archive_dir, filenames, uploader
            )

        self.update_uploaded_filenames(uploaded)



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
    # parser.add_argument(
    #     '--persist-days', '-h',
    #     type=float,
    #     default=30,
    #     help='Number of days to keep uploaded logs before deleting them'
    #     )
    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()

    if not os.path.basename(args.log_archive_dir) == 'archive':
        parser.error(
            'log_archive_dir {!r} does not end in "/archive'.format(
                args.log_archive_dir
                )
            )

    service.configure_logging(args.log_level)

    # Tune down boto logging
    logging.getLogger('boto').setLevel(logging.WARNING)

    upload_service = UploadService(
        args.log_archive_dir,
        args.log_upload_uri
    )

    if args.once:
        service.do_once(upload_service.run)
    else:
        service.do_forever(upload_service.run, service.DEFAULT_INTERVAL)


if __name__ == '__main__':
    main()