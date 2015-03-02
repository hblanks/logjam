""" tests for logjam.compress """

import contextlib
import datetime
import os
import shutil
import tempfile
import unittest

import boto.exception

from logjam.parse import LogFile
import logjam.parse
import logjam.upload


#
# Helpers
#

@contextlib.contextmanager
def named_temporary_dir():
    tempdir = None
    try:
        tempdir = tempfile.mkdtemp()
        yield tempdir
    finally:
        if tempdir is not None:
            shutil.rmtree(tempdir)

def create_logs(logdir, *fnames):
    for fname in fnames:
        with open(os.path.join(logdir, fname), 'w') as f:
            f.write('foo')


#
# Mocks
#

class MockUploader(object):
    """ Mocks logjam.base_uploader.BaseUploader. """

    def __init__(self, upload_uri):
        self.upload_uri = upload_uri
        self.uploaded = set()
        self.not_uploaded = set()
        self.scan_remote_count = 0

    #
    # Methods used by logjam.upload
    #

    def connect(self):
        pass


    def check_uri(self):
        pass


    def scan_remote(self, logfiles):
        self.scan_remote_count += 1
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


    #
    # test_upload_service_run_*
    #

    @contextlib.contextmanager
    def _upload_service(self, filenames):
        with named_temporary_dir() as tempdir:
            tempdir = os.path.join(tempdir, 'archive')
            os.mkdir(tempdir)

            filenames = [
               'flask-20130727T0000Z-i-34aea3fe.log.gz',
               'flask-20130727T0100Z-i-34aea3fe.log.gz',
               'flask-20130727T0200Z-i-34aea3fe.log.gz',
            ]
            create_logs(tempdir, *filenames)

            uploader = MockUploader(DEFAULT_UPLOAD_URI)

            all_logfiles = set(
                logjam.parse.parse_filename(fn) for fn in filenames
                )
            uploader.not_uploaded.update(all_logfiles)

            uploadService = logjam.upload.UploadService(
                tempdir, DEFAULT_UPLOAD_URI, uploader
            )
            yield tempdir, uploader, uploadService


    def test_upload_service_run(self):
        filenames = [
           'flask-20130727T0000Z-i-34aea3fe.log.gz',
           'flask-20130727T0100Z-i-34aea3fe.log.gz',
           'flask-20130727T0200Z-i-34aea3fe.log.gz',
        ]
        with self._upload_service(filenames) as tup:
            tempdir, uploader, uploadService = tup
            uploadService.run()
            marked = os.listdir(os.path.join(tempdir, '.uploaded'))
            assert filenames == marked
            assert set(filenames) == set(
                u.filename for u in uploader.uploaded)
            assert 0 == len(uploader.not_uploaded)

    # make sure that run() doesn't call scan_remote() when it doesn't
    # need to
    def test_upload_service_run_scans_remote_once(self):
        filenames = [
           'flask-20130727T0000Z-i-34aea3fe.log.gz',
           'flask-20130727T0100Z-i-34aea3fe.log.gz',
           'flask-20130727T0200Z-i-34aea3fe.log.gz',
        ]
        with self._upload_service(filenames) as tup:
            tempdir, uploader, uploadService = tup
            assert 0 == uploader.scan_remote_count
            uploadService.run()
            assert 1 == uploader.scan_remote_count
            uploadService.run()
            assert 1 == uploader.scan_remote_count


    def test_upload_service_run_twice(self):
        filenames = [
           'flask-20130727T0000Z-i-34aea3fe.log.gz',
           'flask-20130727T0100Z-i-34aea3fe.log.gz',
           'flask-20130727T0200Z-i-34aea3fe.log.gz',
        ]
        with self._upload_service(filenames) as tup:
            tempdir, uploader, uploadService = tup
            uploadService.run()
            more_filenames = [
               'flask-20130727T0300Z-i-34aea3fe.log.gz',
               'flask-20130727T0400Z-i-34aea3fe.log.gz',
            ]
            create_logs(tempdir, *more_filenames)
            uploader.not_uploaded.update(
                logjam.parse.parse_filename(fn) for fn in more_filenames
            )
            uploadService.run()
            filenames.extend(more_filenames)

            marked = os.listdir(os.path.join(tempdir, '.uploaded'))
            assert filenames == marked
            assert set(filenames) == set(
                u.filename for u in uploader.uploaded)
            assert 0 == len(uploader.not_uploaded)
