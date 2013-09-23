""" tests for logjam.s3_uploader """

import os.path
import unittest

import boto.exception

import logjam.parse
import logjam.s3_uploader


#
# Mocks
#

def _make_mock_function(*return_values):
    """
    Returns a simple mock function which remembers what it was called with,
    and which always returns return_values.
    """
    fn_calls = []
    def mock_function(*args, **kwargs):
        fn_calls.append((args, kwargs))
        if len(return_values) > 1:
            return return_values
        else:
            return return_values[0]
    return mock_function, fn_calls


class MockS3Key(object):
    def __init__(self, name, contents=None):
        self.name = name
        self.contents = contents

    def set_contents_from_filename(self, filename):
        self.contents = 'file:{}'.format(filename)
        self.bucket.keys[self.name] = self


class FailingMockS3Key(MockS3Key):
    def set_contents_from_filename(self, filename):
        raise boto.exception.BotoServerError(500, 'unknown reason')


class MockS3Bucket(object):

    key_class = MockS3Key

    def __init__(self, name, keys):
        self.name = name
        self.keys = dict(
            (key_name, self.key_class(key_name, contents))
            for key_name, contents in keys.iteritems()
            )
        for key in self.keys.itervalues():
            key.bucket = self

    def list(self, prefix):
        return [
            self.keys[k] for k in self.keys if k.startswith(prefix)
            ]

    def get_key(self, name):
        return self.keys.get(name)

    def new_key(self, name):
        key = self.key_class(name)
        key.bucket = self
        return key



class FailingMockS3Bucket(MockS3Bucket):
    key_class = FailingMockS3Key


class MockS3Connection(object):
    bucket_class = MockS3Bucket

    def __init__(self, buckets, bucket_class=None):
        if bucket_class:
            self.bucket_class = bucket_class
        self.buckets = dict(
            (bucket_name, self.bucket_class(bucket_name, keys))
            for bucket_name, keys in buckets.iteritems()
            )

    def get_bucket(self, name):
        if name not in self.buckets:
            raise boto.exception.S3ResponseError(404, 'Bucket not found')
        return self.buckets[name]


#
# Tests for the helper functions in s3_uploader.py
#

class TestS3UploaderHelpers(unittest.TestCase):

    #
    # test_get_ec2_metadata_*
    #

    def test_get_ec2_metadata_returns(self):
        # We can't test the result much, because we don't know if this
        # test is running from EC2. But, at least we check that it returns
        # None or a dict containing 'iam'
        actual = logjam.s3_uploader._get_ec2_metadata(timeout=0.01)
        self.assertTrue(actual is None or isinstance(actual, dict))
        if isinstance(actual, dict):
            self.assertIn('iam', actual)
            self.assertIn('security-credentials', actual['iam'])
            self.assertIsInstance(actual['iam']['security-credentials'], dict)


    #
    # test_get_iam_role_*
    #

    def test_get_iam_role_has_cred(self):
        def get_ec2_metadata():
            return {
                'iam': {
                    'security-credentials': {
                        'FOO': {
                            'AccessKeyId': 'ID',
                            'SecretAccessKey': 'KEY',
                            'Token': 'TOKEN',
                            }
                        }
                    }
                }

        expected = ('ID', 'KEY', 'TOKEN')
        actual = logjam.s3_uploader._get_iam_role(get_ec2_metadata)
        self.assertEqual(expected, actual)


    def test_get_iam_role_lacks_cred(self):
        def get_ec2_metadata():
            return {'iam': {'security-credentials': {}}}

        expected = None
        actual = logjam.s3_uploader._get_iam_role(get_ec2_metadata)
        self.assertEqual(expected, actual)


    def test_get_iam_role_lacks_metadata(self):
        def get_ec2_metadata():
            return None

        expected = None
        actual = logjam.s3_uploader._get_iam_role(get_ec2_metadata)
        self.assertEqual(expected, actual)


    #
    # test_get_s3_endpoint
    #

    def test_get_s3_endpoint_has_aws_default_region(self):
        os_environ = {'AWS_DEFAULT_REGION': 'us-west-1'}
        def get_ec2_metadata():
            raise NotImplementedError
        expected = 's3-us-west-1.amazonaws.com'

        actual = logjam.s3_uploader._get_s3_endpoint(os_environ, get_ec2_metadata)
        self.assertEqual(expected, actual)


    def test_get_s3_endpoint_has_ec2_metadata(self):
        os_environ = {}
        def get_ec2_metadata():
            return {
                'placement': {
                    'availability-zone': 'us-west-2a'
                    }
                }
        expected = 's3-us-west-2.amazonaws.com'

        actual = logjam.s3_uploader._get_s3_endpoint(os_environ, get_ec2_metadata)
        self.assertEqual(expected, actual)

    def test_get_s3_endpoint_default_endpoint(self):
        os_environ = {}
        get_ec2_metadata, calls = _make_mock_function(None)

        expected = 's3.amazonaws.com'
        expected_calls = [
            (tuple(), {})
            ]
        actual = logjam.s3_uploader._get_s3_endpoint(os_environ, get_ec2_metadata)
        self.assertEqual(expected, actual)
        self.assertEqual(expected_calls, calls)

    #
    # test_connect_s3
    #


    def test_connect_s3_from_environ_only(self):
        os_environ = {
            'AWS_DEFAULT_REGION': 'eu-west-1',
            'AWS_ACCESS_KEY_ID': 'ID',
            'AWS_SECRET_ACCESS_KEY': 'SECRET',
            }
        def get_ec2_metadata():
            raise NotImplementedError
        boto_connect_s3, calls = _make_mock_function("Connection")

        expected = "Connection"
        expected_calls = [
            (tuple(), {'host': 's3-eu-west-1.amazonaws.com'}),
            ]
        actual = logjam.s3_uploader._connect_s3(
            os_environ, get_ec2_metadata, boto_connect_s3)
        self.assertEqual(expected, actual)
        self.assertEqual(expected_calls, calls)


    def test_connect_s3_from_ec2_metadata_only(self):
        os_environ = {}
        get_ec2_metadata, get_ec2_metadata_calls = _make_mock_function(
            {
                'iam': {
                    'security-credentials': {
                        'FOO': {
                            'AccessKeyId': 'ID',
                            'SecretAccessKey': 'KEY',
                            'Token': 'TOKEN',
                            }
                        }
                    },
                'placement': {
                    'availability-zone': 'us-west-2a'
                    },
            })

        calls = []
        def boto_connect_s3(*args, **kwargs):
            calls.append((args, kwargs))
            if len(args) == 0:
                raise boto.exception.NoAuthHandlerFound
            return "Connection"

        expected = "Connection"
        expected_get_ec2_metadata_calls = [
            (tuple(), {}),
            (tuple(), {}),
            ]
        expected_calls = [
            # the first, failed call b/c no creds in os.environ
            (tuple(), {'host': 's3-us-west-2.amazonaws.com'}),
            # the second, successful call w/ec2 metadata
            (
                ('ID', 'KEY'),
                {
                    'host': 's3-us-west-2.amazonaws.com',
                    'security_token': 'TOKEN',
                }
            ),
            ]
        actual = logjam.s3_uploader._connect_s3(
            os_environ, get_ec2_metadata, boto_connect_s3)
        self.assertEqual(expected, actual)
        self.assertEqual(expected_get_ec2_metadata_calls, get_ec2_metadata_calls)
        self.assertEqual(expected_calls, calls)


    def test_get_logfile_uri_valid_uri(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        filename = 'haproxy-20130727T0100Z-i-34aea3fe.log.gz'
        logfile = logjam.parse.parse_filename(filename)

        expected = 's3://nt8.logs.us-west-2/haproxy/2013/07/27/' + filename
        actual = logjam.s3_uploader.get_logfile_uri(upload_uri, logfile)
        self.assertEqual(expected, actual)

    def _assert_get_logfile_uri_raises(self, upload_uri, missing_fields):
        filename = 'haproxy-20130727T0100Z-i-34aea3fe.log.gz'
        logfile = logjam.parse.parse_filename(filename)
        pattern = '^upload_uri lacks mandatory fields: {}$'.format(missing_fields)
        with self.assertRaisesRegexp(ValueError, pattern):
            logjam.s3_uploader.get_logfile_uri(upload_uri, logfile)

    def test_get_logfile_uri_missing_prefix(self):
        upload_uri = 's3://nt8.logs.us-west-2/{year}/{month}/{day}/{filename}'
        self._assert_get_logfile_uri_raises(upload_uri, 'prefix')

    def test_get_logfile_uri_missing_year(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{month}/{day}/{filename}'
        self._assert_get_logfile_uri_raises(upload_uri, 'year')

    def test_get_logfile_uri_missing_month(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{day}/{filename}'
        self._assert_get_logfile_uri_raises(upload_uri, 'month')

    def test_get_logfile_uri_missing_day(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{filename}'
        self._assert_get_logfile_uri_raises(upload_uri, 'day')


    #
    # test_get_parent_dir_uris_*
    #

    def test_get_parent_dir_uris_unrelated_parents(self):
        logfile_uris = [
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0000Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0100Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0200Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/flask/2013/07/26/flask-20130727T0000Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/flask/2013/07/26/flask-20130727T0100Z-i-34aea3fe.log.gz',
            ]
        expected = [
            's3://nt8.logs.us-west-2/flask/2013/07/26/',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/',
            ]
        actual = sorted(logjam.s3_uploader.get_parent_dir_uris(logfile_uris))
        self.assertEqual(expected, actual)


    def test_get_parent_dir_uris_single_parent(self):
        logfile_uris = [
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0000Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0100Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0200Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0300Z-i-34aea3fe.log.gz',
            ]
        expected = [
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/',
            ]
        actual = sorted(logjam.s3_uploader.get_parent_dir_uris(logfile_uris))
        self.assertEqual(expected, actual)


    def test_get_parent_dir_uris_related_parents(self):
        logfile_uris = [
            's3://nt8.logs.us-west-2/haproxy/2013/07/26/haproxy-20130726T2200Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/26/haproxy-20130726T2300Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0000Z-i-34aea3fe.log.gz',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0100Z-i-34aea3fe.log.gz',
            ]
        expected = [
            's3://nt8.logs.us-west-2/haproxy/2013/07/26/',
            's3://nt8.logs.us-west-2/haproxy/2013/07/27/',
            ]
        actual = sorted(logjam.s3_uploader.get_parent_dir_uris(logfile_uris))
        self.assertEqual(expected, actual)


#
# Tests for s3_uploader.S3Uploader
#

class TestS3Uploader(unittest.TestCase):

    #
    # Helpers
    #

    def _make_uploader(self, upload_uri, buckets, bucket_class=None):
        """
        Makes an S3Uploader, bound to a MockS3Connection containing a
        given dict of MockS3Buckets.
        """
        s3_conn = MockS3Connection(buckets, bucket_class=bucket_class)

        def connect_s3():
            return s3_conn

        def storage_uri_for_key(key):
            return 's3://{}/{}'.format(key.bucket.name, key.name)

        if upload_uri is None:
            upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'

        uploader = logjam.s3_uploader.S3Uploader(
            upload_uri,
            connect_s3=connect_s3,
            storage_uri_for_key=storage_uri_for_key)
        uploader.connect()

        return uploader

    #
    # test_check_uri_*
    #

    def test_check_uri_valid_uri(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(upload_uri, {'nt8.logs.us-west-2': {}})
        expected = None
        actual = uploader.check_uri()
        self.assertEqual(expected, actual)

    def test_check_uri_invalid_uri(self):
        upload_uri = 's3://nt8.logs.us-west-2/{year}/{month}/{day}/'
        uploader = self._make_uploader(upload_uri, {'nt8.logs.us-west-2': {}})
        expected = 'upload_uri lacks mandatory fields: filename, prefix'
        actual = uploader.check_uri()
        self.assertEqual(expected, actual)


    def test_check_uri_missing_bucket(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(upload_uri, {'nt8.logs.us-east-1': {}})
        expected = 'Failed to find bucket for {}'.format(upload_uri)
        actual = uploader.check_uri()
        self.assertEqual(expected, actual)



    #
    # test_scan_remote_*
    #

    def test_scan_remote_empty_bucket(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(upload_uri, {'nt8.logs.us-west-2': {}})

        pf = logjam.parse.parse_filename
        logfiles = [
            pf('haproxy-20130726T2200Z-i-34aea3fe.log.gz'),
            ]

        uploaded, not_uploaded = uploader.scan_remote(logfiles)

        self.assertEqual(set(), uploaded)
        self.assertEqual(set(logfiles), not_uploaded)

    def test_scan_remote_partial_bucket(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(
            upload_uri,
            {'nt8.logs.us-west-2': {
                'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz': '1',
                'flask/2013/07/27/flask-20130727T0100Z-i-34aea3fe.log.gz': '2',
                'haproxy/2013/07/26/haproxy-20130726T2200Z-i-34aea3fe.log.gz': '3',
                'haproxy/2013/07/26/haproxy-20130726T2300Z-i-34aea3fe.log.gz': '4',
                }
            }
            )

        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('flask-20130727T0100Z-i-34aea3fe.log.gz'),
            pf('flask-20130727T0200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2300Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130727T0100Z-i-34aea3fe.log.gz'),
            ]

        expected_uploaded = set([
            pf('flask-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('flask-20130727T0100Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2300Z-i-34aea3fe.log.gz'),
            ])
        expected_not_uploaded = set([
            pf('flask-20130727T0200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130727T0100Z-i-34aea3fe.log.gz'),
            ])

        uploaded, not_uploaded = uploader.scan_remote(logfiles)

        self.assertEqual(expected_uploaded, uploaded)
        self.assertEqual(expected_not_uploaded, not_uploaded)


    def test_scan_remote_complete_bucket(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(
            upload_uri,
            {'nt8.logs.us-west-2': {
                'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz': '1',
                'flask/2013/07/27/flask-20130727T0100Z-i-34aea3fe.log.gz': '2',
                'haproxy/2013/07/26/haproxy-20130726T2200Z-i-34aea3fe.log.gz': '3',
                'haproxy/2013/07/26/haproxy-20130726T2300Z-i-34aea3fe.log.gz': '4',
                }
            }
            )

        pf = logjam.parse.parse_filename
        logfiles = [
            pf('flask-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('flask-20130727T0100Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2300Z-i-34aea3fe.log.gz'),
            ]

        expected_uploaded = set([
            pf('flask-20130727T0000Z-i-34aea3fe.log.gz'),
            pf('flask-20130727T0100Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2200Z-i-34aea3fe.log.gz'),
            pf('haproxy-20130726T2300Z-i-34aea3fe.log.gz'),
            ])
        expected_not_uploaded = set()

        uploaded, not_uploaded = uploader.scan_remote(logfiles)

        self.assertEqual(expected_uploaded, uploaded)
        self.assertEqual(expected_not_uploaded, not_uploaded)


    #
    # test_upload_logfile_*
    #

    def test_upload_logfile_upload_success(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(
            upload_uri, {'nt8.logs.us-west-2': {}}
            )

        pf = logjam.parse.parse_filename
        logfile = pf('flask-20130727T0000Z-i-34aea3fe.log.gz')
        log_archive_dir = '/does/not/exist/var/log/archive'

        expected_key_name = 'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz'
        expected_key_contents = 'file:{}'.format(
            os.path.join(log_archive_dir, logfile.filename)
            )

        error = uploader.upload_logfile(log_archive_dir, logfile)
        self.assertIsNone(error)

        key = uploader.s3_conn.get_bucket('nt8.logs.us-west-2').get_key(
            expected_key_name)
        self.assertIsNotNone(key)
        self.assertEqual(expected_key_contents, key.contents)


    def test_upload_logfile_upload_exists_already(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(
            upload_uri,
            {'nt8.logs.us-west-2': {
                'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz': 'SOMETHING ELSE'
                }
            }
            )

        pf = logjam.parse.parse_filename
        logfile = pf('flask-20130727T0000Z-i-34aea3fe.log.gz')
        log_archive_dir = '/does/not/exist/var/log/archive'

        expected_key_name = 'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz'
        expected_key_contents = 'SOMETHING ELSE'

        error = uploader.upload_logfile(log_archive_dir, logfile)
        self.assertIsNone(error)

        key = uploader.s3_conn.get_bucket('nt8.logs.us-west-2').get_key(
            expected_key_name)
        self.assertIsNotNone(key)
        self.assertEqual(expected_key_contents, key.contents)


    def test_upload_logfile_upload_fails(self):
        upload_uri = 's3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'
        uploader = self._make_uploader(
            upload_uri,
            {'nt8.logs.us-west-2': {}},
            bucket_class=FailingMockS3Bucket
            )

        pf = logjam.parse.parse_filename
        logfile = pf('flask-20130727T0000Z-i-34aea3fe.log.gz')
        log_archive_dir = '/does/not/exist/var/log/archive'

        expected_key_name = 'flask/2013/07/27/flask-20130727T0000Z-i-34aea3fe.log.gz'

        error = uploader.upload_logfile(log_archive_dir, logfile)
        self.assertIsInstance(error, boto.exception.BotoServerError)
