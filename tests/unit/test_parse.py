""" tests for logjam.parse """

import datetime
import unittest

from logjam.parse import LogFile
import logjam.parse


class TestParse(unittest.TestCase):

    #
    # test_parse_filename_*
    #

    def test_parse_filename_valid_hour_minute_and_suffix(self):
        filename = 'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'
        expected = LogFile(
            'flask-requests',
            datetime.datetime(2013, 7, 27, 12, 0),
            'us-west-2-i-ae23fega',
            '.log',
            filename,
            )
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    def test_parse_filename_valid_hour_minute_no_suffix(self):
        filename = 'flask-requests-20130727T1200Z.log'
        expected = (
            'flask-requests',
            datetime.datetime(2013, 7, 27, 12, 0),
            None,
            '.log',
            filename,
            )
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    def test_parse_filename_missing_iso8601(self):
        filename = 'flask-requests-us-west-2-i-ae23fega.log'
        expected = None
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    def test_parse_filename_invalid_iso8601(self):
        filename = 'flask-requests-20131301T1200Z-us-west-2-i-ae23fega.log'
        expected = None
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    def test_parse_filename_missing_prefix(self):
        filename = '20130727T1200Z-us-west-2-i-ae23fega.log'
        expected = None
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    def test_parse_filename_missing_extension(self):
        filename = 'flask-requests-20130727T1200Z-us-west-2-i-ae23fega'
        expected = None
        actual = logjam.parse.parse_filename(filename)
        self.assertEqual(expected, actual)


    #
    # test_group_filenames_*
    #

    def test_group_filenames_two_groups(self):
        filenames = [
            'flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log',
            'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0000Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0100Z-us-west-2-i-ae23fega.log',
            ]
        pf = logjam.parse.parse_filename
        expected = {
            ('flask-requests', 'us-west-2-i-ae23fega', '.log'): [
                pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
                pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
                ],
            ('haproxy', 'us-west-2-i-ae23fega', '.log'): [
                pf('haproxy-20130727T0000Z-us-west-2-i-ae23fega.log'),
                pf('haproxy-20130727T0100Z-us-west-2-i-ae23fega.log'),
                ],
            }
        actual = logjam.parse.group_filenames(filenames)
        self.assertEqual(expected, actual)


    def test_group_filenames_two_groups_one_non_hourly(self):
        filenames = [
            'flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log',
            'flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0000Z-us-west-2-i-ae23fega.log',
            'haproxy-20130727T0100Z-us-west-2-i-ae23fega.log',
            'messages',
            ]
        pf = logjam.parse.parse_filename
        expected = {
            ('flask-requests', 'us-west-2-i-ae23fega', '.log'): [
                pf('flask-requests-20130727T1200Z-us-west-2-i-ae23fega.log'),
                pf('flask-requests-20130727T1300Z-us-west-2-i-ae23fega.log'),
                ],
            ('haproxy', 'us-west-2-i-ae23fega', '.log'): [
                pf('haproxy-20130727T0000Z-us-west-2-i-ae23fega.log'),
                pf('haproxy-20130727T0100Z-us-west-2-i-ae23fega.log'),
                ],
            }
        actual = logjam.parse.group_filenames(filenames)
        self.assertEqual(expected, actual)
