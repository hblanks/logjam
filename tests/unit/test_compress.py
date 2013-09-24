""" tests for logjam.compress """

import tempfile
import datetime
import os
import unittest


from logjam.parse import LogFile
import logjam.parse
import logjam.compress


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
    # test_open_excl_* --- not used
    #

    #     def test_open_excl_success(self):
    #         t = None
    #         try:
    #             with tempfile.NamedTemporaryFile() as t:
    #                 pass
    #             with logjam.compress.open_excl(t.name, 'w') as f:
    #                 f.write('foo')
    #         finally:
    #             if t and os.path.isfile(t.name):
    #                 os.unlink(t.name)
    #
    #
    #     def test_open_excl_fail(self):
    #         with self.assertRaisesRegexp(OSError, 'File exists: .+'):
    #             t = None
    #             try:
    #                 with tempfile.NamedTemporaryFile(delete=False) as t:
    #                     pass
    #                 with logjam.compress.open_excl(t.name, 'wb'):
    #                     t.write('foo')
    #             finally:
    #                 if t and os.path.isfile(t.name):
    #                     os.unlink(t.name)

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
        t = expected = None
        os_rename, rename_args = self._make_os_rename()
        try:
            with tempfile.NamedTemporaryFile(delete=False) as t:
                archive_dir = os.path.join(os.path.dirname(t.name), 'archive')
                expected = os.path.join(
                    archive_dir,
                    os.path.basename(t.name) + '.gz'
                    )
                actual = logjam.compress.compress_path(
                    t.name,
                    ('true',),
                    '.gz',
                    archive_dir,
                    os_rename=os_rename
                    )
                self.assertEqual(expected, actual)
                self.assertEqual([expected], rename_args[1:])

                # ... and check that compress_path() deleted the
                # original file.
                self.assertFalse(os.path.isfile(t.name)
        finally:
            if t and os.path.isfile(t.name):
                os.unlink(t.name)
            if expected and os.path.isfile(expected):
                os.unlink(expected)


    def test_compress_path_cmd_fail(self):
        t = not_expected = None
        os_rename, rename_args = self._make_os_rename()
        try:
            with tempfile.NamedTemporaryFile() as t:
                archive_dir = os.path.join(os.path.dirname(t.name), 'archive')
                expected = None
                actual = logjam.compress.compress_path(
                    t.name,
                    ('false',),
                    '.gz',
                    archive_dir,
                    os_rename=os_rename
                    )
                self.assertEqual(expected, actual)
                self.assertEqual([None], rename_args)

                # ... and check that compress_path() did NOT delete the
                # original file.
                self.assertTrue(os.path.isfile(t.name)
        finally:
            if rename_args[0] and os.path.isfile(rename_args[0]):
                os.unlink(rename_args[0])
