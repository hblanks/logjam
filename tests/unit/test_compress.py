""" tests for logjam.compress """

import contextlib
import datetime
import os
import os.path
import shutil
import tempfile
import unittest

from logjam.parse import LogFile
import logjam.compress
import logjam.parse


@contextlib.contextmanager
def temporary_directory():
    tempdir = None
    try:
        tempdir = tempfile.mkdtemp()
        yield tempdir
    finally:
        if tempdir and os.path.isdir(tempdir):
            shutil.rmtree(tempdir)


class TestCompress(unittest.TestCase):

    maxDiff = None

    #
    # test_select_superseded_by_new_file_*
    #

    def test_select_superseded_by_new_file_three_files(self):
        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-requests-20130727T1400Z-us-west-2-i-ae23fega.log'),
            pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
            pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
            ]
        expected = [
            pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
            pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
            ]
        actual = logjam.compress.select_superseded_by_new_file(logfiles)
        self.assertEqual(expected, actual)


    def test_select_superseded_by_new_file_one_file(self):
        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-requests-20130727T1400Z-us-west-2-i-ae23fega.log'),
            ]
        expected = []
        actual = logjam.compress.select_superseded_by_new_file(logfiles)
        self.assertEqual(expected, actual)


    #
    # test_select_superseded_time_*
    #

    def test_select_superseded_by_timestamp_one_superseded_file(self):
        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-requests-20130727T1400Z-us-west-2-i-ae23fega.log'),
            pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
            ]
        current_time = datetime.datetime(2013, 07, 27, 14, 5, 1)
        expected = [
            pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
            ]
        actual = logjam.compress.select_superseded_by_timestamp(
            logfiles, current_time
            )
        self.assertEqual(expected, actual)


    def test_select_superseded_by_timestamp_no_superseded_files(self):
        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-requests-20130727T1400Z-us-west-2-i-ae23fega.log'),
            pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
            ]
        current_time = datetime.datetime(2013, 07, 27, 14, 3)
        expected = []
        actual = logjam.compress.select_superseded_by_timestamp(
            logfiles, current_time
            )
        self.assertEqual(expected, actual)


    #
    # test_yield_old_logfiles_*
    #

    def test_yield_old_logfiles_two_groups_three_old(self):
        pf = logjam.parse.parse_filename
        filenames = [
            'flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log',
            'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0000Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0100Z-us-west-2-i-ae23fega.log',
            ]
        timestamp = datetime.datetime(2013, 07, 27, 13, 36)

        expected = [
            pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
            pf('haproxy-20130727T0000Z-us-west-2-i-ae23fega.log'),
            pf('haproxy-20130727T0100Z-us-west-2-i-ae23fega.log'),
            ]
        actual = list(
            logjam.compress.yield_old_logfiles(filenames, timestamp)
            )
        self.assertEqual(expected, actual)


    def test_yield_old_logfiles_two_groups_three_old_one_non_hourly(self):
        pf = logjam.parse.parse_filename
        filenames = [
            'flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log',
            'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0000Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0100Z-us-west-2-i-ae23fega.log',
            'messages'
            ]
        timestamp = datetime.datetime(2013, 07, 27, 13, 36)

        expected = [
            pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
            pf('haproxy-20130727T0000Z-us-west-2-i-ae23fega.log'),
            pf('haproxy-20130727T0100Z-us-west-2-i-ae23fega.log'),
            ]
        actual = list(
            logjam.compress.yield_old_logfiles(filenames, timestamp)
            )
        self.assertEqual(expected, actual)


    def test_yield_old_logfiles_two_groups_no_old(self):
        pf = logjam.parse.parse_filename
        filenames = [
            'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T1200Z-us-west-2-i-ae23fega.log',
            ]
        timestamp = datetime.datetime(2013, 07, 27, 12, 55)

        expected = []
        actual = list(
            logjam.compress.yield_old_logfiles(filenames, timestamp)
            )
        self.assertEqual(expected, actual)


    #
    # test_duplicate_timestamp_path
    #

    def test_duplicate_timestamp_path(self):
        path = \
            '/a/flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'
        expected = (
            '/a/flask-requests-'
            'logjam-compress-duplicate-timestamp-'
            '20130727T1200Z-us-west-2-i-ae23fega.log'
        )
        actual = logjam.compress.duplicate_timestamp_path(path)
        self.assertEqual(expected, actual)

    def test_duplicate_timestamp_path_duplicates_exists(self):
        with temporary_directory() as temp_dir:
            path = os.path.join(
                temp_dir,
                'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'
            )
            dup_path = logjam.compress.duplicate_timestamp_path(path)
            with open(dup_path, 'w'):
                pass

            expected = dup_path.replace(
                'logjam-compress-duplicate-timestamp-',
                'logjam-compress-duplicate-timestamp-01-'
            )
            actual = logjam.compress.duplicate_timestamp_path(path)
            self.assertEqual(expected, actual)

            for i in range(1, 24):
                next_path = dup_path.replace(
                    'logjam-compress-duplicate-timestamp-',
                    'logjam-compress-duplicate-timestamp-%02d-' % i
                )
                with open(next_path, 'w'):
                    pass

            expected = dup_path.replace(
                'logjam-compress-duplicate-timestamp-',
                'logjam-compress-duplicate-timestamp-24-'
            )
            actual = logjam.compress.duplicate_timestamp_path(path)
            self.assertEqual(expected, actual)

            next_path = dup_path.replace(
                'logjam-compress-duplicate-timestamp-',
                'logjam-compress-duplicate-timestamp-24-'
            )
            with open(next_path, 'w'):
                pass

            expected_pat = '^25 duplicate timestamp paths detected.$'
            with self.assertRaisesRegexp(Exception, expected_pat):
                logjam.compress.duplicate_timestamp_path(path)


    #
    # test_compress_path*
    #

    def _make_os_rename(self):
        """ Helper for mocking os.rename. """
        rename_args = [None]
        def os_rename(*args):
            rename_args[:] = args
        return os_rename, rename_args

    def test_compress_path_cmd_success(self):
        os_rename, rename_args = self._make_os_rename()
        with temporary_directory() as temp_dir:
            log_path = os.path.join(temp_dir, 'compress-test.log')
            with open(log_path, 'w') as f:
                f.write('compress-test')

            archive_dir = os.path.join(temp_dir, 'archive')
            expected = os.path.join(
                archive_dir,
                os.path.basename(log_path) + '.gz'
                )
            actual = logjam.compress.compress_path(
                log_path,
                ('true',),
                '.gz',
                archive_dir,
                os_rename=os_rename
                )
            self.assertEqual(expected, actual)
            self.assertEqual([expected], rename_args[1:])

            # ... and check that compress_path() deleted the
            # original file.
            self.assertFalse(os.path.isfile(log_path))


    def test_compress_path_cmd_fail(self):
        os_rename, rename_args = self._make_os_rename()
        with temporary_directory() as temp_dir:
            log_path = os.path.join(temp_dir, 'compress-test.log')
            with open(log_path, 'w') as f:
                f.write('compress-test')

            archive_dir = os.path.join(temp_dir, 'archive')
            expected = None
            actual = logjam.compress.compress_path(
                log_path,
                ('false',),
                '.gz',
                archive_dir,
                os_rename=os_rename
                )

            self.assertEqual(expected, actual)
            self.assertEqual([None], rename_args)

            # ... and check that compress_path() did NOT delete the
            # original file.
            self.assertTrue(os.path.isfile(log_path))

    def test_compress_path_preexisting_dst_path(self):
        os_rename, rename_args = self._make_os_rename()
        with temporary_directory() as temp_dir:
            fname = 'compress-test-20140411T0000Z.log'
            log_path = os.path.join(temp_dir, fname)
            with open(log_path, 'w') as f:
                f.write('compress-test')

            archive_dir = os.path.join(temp_dir, 'archive')
            os.mkdir(archive_dir)
            existing_path = os.path.join(archive_dir, fname + '.gz')
            with open(existing_path, 'w') as f:
                f.write('compress-test')

            expected = os.path.join(
                archive_dir,
                'compress-test-logjam-compress-duplicate-timestamp-20140411T0000Z.log.gz'
            )
            actual = logjam.compress.compress_path(
                log_path,
                ('true',),
                '.gz',
                archive_dir,
                os_rename=os_rename
            )
            self.assertEqual(expected, actual)

            # check that compress_path() would have moved the compressed
            # file into place
            self.assertEqual([expected], rename_args[1:])

            # check that compress_path() did not change the preexisting
            # compressed file
            self.assertTrue(os.path.isfile(existing_path))
            with open(existing_path, 'r') as f:
                self.assertEqual('compress-test', f.read())

            # check that compress_path() deleted the source log file.
            self.assertFalse(os.path.isfile(log_path))
