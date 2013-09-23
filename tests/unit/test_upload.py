""" tests for logjam.compress """

import tempfile
import datetime
import os
import unittest

import boto.exception

from logjam.parse import LogFile
import logjam.parse
import logjam.upload


#
# Mocks
#

class MockUploader(object):

    def __init__(self, upload_uri):
        self.upload_uri = upload_uri
        self.uploaded = set()
        self.not_uploaded = set()

    #
    # Methods used by logjam.upload
    #

    def connect():
        raise NotImplementedError


    def check_uri():
        raise NotImplementedError


    def scan_remote(self, logfiles):
        # make copies! the caller will be mutating them
        return set(self.uploaded), set(self.not_uploaded)


    def upload_logfile(self, log_archive_dir, logfile):
        self.uploaded.add(logfile)
        self.not_uploaded.remove(logfile)


class FailingMockUploader(MockUploader):

    def upload_logfile(self, log_archive_dir, logfile):
        return boto.exception.BotoServerError(500, 'unknown reason')

DEFAULT_UPLOAD_URI ='s3://logs.us-east-1/{prefix}/{year}/{month}/{day}/{filename}'


class TestUpload(unittest.TestCase):

    maxDiff = None


    #
    # test_scan_and_upload_filenames_*
    #

    def test_scan_and_upload_filenames_no_filenames(self):
        uploader = MockUploader(DEFAULT_UPLOAD_URI)
        uploaded, not_uploaded = logjam.upload.scan_and_upload_filenames(
            '/does/not/exist/log/archive',
            [],
            uploader
            )
        self.assertEqual(set(), uploaded)
        self.assertEqual(set(), not_uploaded)

        # Make sure upload_logfile was not called
        self.assertEqual(set(), uploader.uploaded)


    def test_scan_and_upload_filenames_three_filenames(self):
        uploader = MockUploader(DEFAULT_UPLOAD_URI)
        filenames = [
            'flask-20130727T0000Z-i-34aea3fe.log.gz',
            'flask-20130727T0100Z-i-34aea3fe.log.gz',
            'flask-20130727T0200Z-i-34aea3fe.log.gz',
            ]

        all_logfiles = set(
            logjam.parse.parse_filename(fn) for fn in filenames
            )

        uploader.not_uploaded.update(all_logfiles)

        uploaded, not_uploaded = logjam.upload.scan_and_upload_filenames(
            '/does/not/exist/log/archive',
            filenames,
            uploader
            )
        self.assertEqual(all_logfiles, uploaded)
        self.assertEqual(set(), not_uploaded)

        # Make sure upload_logfile was not called
        self.assertEqual(all_logfiles, uploader.uploaded)
        self.assertEqual(set(), uploader.not_uploaded)

    def test_scan_and_upload_one_failure(self):

        uploader = FailingMockUploader(DEFAULT_UPLOAD_URI)
        filenames = [
            'flask-20130727T0000Z-i-34aea3fe.log.gz',
            'flask-20130727T0100Z-i-34aea3fe.log.gz',
            'flask-20130727T0200Z-i-34aea3fe.log.gz',
            ]

        uploader.uploaded.update(
            logjam.parse.parse_filename(fn) for fn in filenames[:-1]
            )
        uploader.not_uploaded.add(
            logjam.parse.parse_filename(filenames[-1])
            )

        expected_uploaded = set(uploader.uploaded)
        expected_not_uploaded = set(uploader.not_uploaded)

        uploaded, not_uploaded = logjam.upload.scan_and_upload_filenames(
            '/does/not/exist/log/archive',
            filenames,
            uploader
            )
        self.assertEqual(expected_uploaded, uploaded)
        self.assertEqual(expected_not_uploaded, not_uploaded)
