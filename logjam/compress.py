"""
Logfile compressor
"""

from __future__ import absolute_import

import argparse
import contextlib
import datetime
import logging
import os
import os.path
import subprocess
import tempfile

from . import parse
from . import service
from .parse import LogFile

COMMAND_DESCRIPTION = \
"""Takes a directory of ISO8601 logfiles. Compresses any superseded logs
and stores them into an archive/ directory therein.

Sample usage:

    logjam-compress /var/log/hourly/

"""

ONE_HOUR_PLUS = datetime.timedelta(hours=1, minutes=5)

#
# Helpers
#

def select_superseded_by_new_file(logfiles):
    if len(logfiles) > 1:
        return sorted(logfiles, key=parse.logfile_keyfunc)[:-1]
    return []


def select_superseded_by_timestamp(logfiles, current_timestamp):
    return [
        lf for lf in logfiles
        if current_timestamp - lf.timestamp > ONE_HOUR_PLUS
        ]


def yield_old_logfiles(filenames, current_timestamp):
    logfiles_by_group = parse.group_filenames(filenames)
    # logging.debug('compress.yield_old_logfiles: group %r',
    #     logfiles_by_group
    #     )
    for logfiles in logfiles_by_group.itervalues():
        old_logfiles = set(
            select_superseded_by_new_file(logfiles)
            )
        old_logfiles.update(
            select_superseded_by_timestamp(logfiles, current_timestamp)
            )
        for logfile in sorted(old_logfiles, key=parse.logfile_keyfunc):
            yield logfile


# @contextlib.contextmanager
# def open_excl(path, mode):
#     """
#     Implements an open() that only works if the specified file
#     does not yet exist.
#     """
#     fd = f = None
#     try:
#         fd = os.open(path, os.O_EXCL|os.O_CREAT|os.O_WRONLY)
#         print fd
#         f = os.fdopen(fd, mode)
#         yield f
#     finally:
#         if f:
#             f.close()
#         elif fd:
#             os.close(fd)
#


def compress_path(
    path, compress_cmd_args, compress_extension, archive_dir, os_rename=os.rename,
    ):
    log_dir = os.path.dirname(path)
    log_filename = os.path.basename(path)
    dst_path = os.path.join(archive_dir, log_filename + compress_extension)
    f = None
    try:
        with tempfile.NamedTemporaryFile(
            'wb', dir=log_dir, prefix=log_filename + '.', delete=False
            ) as f:
            args = compress_cmd_args + (path,)
            logging.debug('compress.compress_path: %s', ' '.join(args))
            p = subprocess.Popen(args, stdout=f)
            retcode = p.wait() # set timeout?

            if retcode:
                logging.error('compress.compress_path: %s exited %d',
                    ' '.join(compress_cmd_args), retcode
                    )
                return

        os_rename(f.name, dst_path)

        if os.path.isfile(path):
            os.unlink(path)

    finally:
        if f and os.path.isfile(f.name):
            os.unlink(f.name)

    return dst_path




#
# Core functions
#

def scan_and_compress(log_dir, compress_cmd_args, compress_extension):

    logging.debug('compress.scan_and_compress: %r %r %r',
        log_dir, compress_cmd_args, compress_extension
        )
    archive_dir = os.path.join(log_dir, 'archive')
    if not os.path.isdir(archive_dir):
        os.mkdir(archive_dir)

    filenames = os.listdir(log_dir)
    current_timestamp = datetime.datetime.utcnow()
    for logfile in yield_old_logfiles(filenames, current_timestamp):
        compressed_path = compress_path(
            os.path.join(log_dir, logfile.filename),
            compress_cmd_args,
            compress_extension,
            archive_dir,
            )



#
# CLI functions
#

def make_parser():
    parser = argparse.ArgumentParser(
        description=COMMAND_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter
        )
    parser.add_argument(
        'log_dir',
        help='Directory in which to scan for, and compress, hourly log files',
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

    compress_cmd_args = ('gzip', '-c')
    compress_extension = '.gz'

    service.configure_logging(args.log_level)

    if args.once:
        scan_and_compress(args.log_dir, compress_cmd_args, compress_extension)
    else:
        service.do_forever(
            scan_and_compress,
            service.DEFAULT_INTERVAL,
            args.log_dir, compress_cmd_args, compress_extension
            )
