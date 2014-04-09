"""
Uploader for uploading logs to S3.
"""


from __future__ import absolute_import

import json
import logging
import os
import socket
import string

# NB: Although not used directly, boto.storage_uri_for_key() depends on
# boto.s3.key being imported.
import boto
import boto.s3
import boto.s3.key
import boto.exception

from .base_uploader import BaseUploader
from . import parse

#
# S3 connection helpers
#

EC2_METADATA_TIMEOUT = 0.2

def _get_ec2_metadata(timeout=EC2_METADATA_TIMEOUT):
    """
    Wraps boto.utils.get_instance_metadata(), fetching local EC2
    instance metadata without waiting a long time for socket timeout.
    Returns a dict if available, else None.
    """

    # earlier versions of boto (including 2.2.2, which ships with Ubuntu 12.04
    # LTS) don't allow a timeout when looking up EC2 instance metadata, so
    # we check ourselves.
    try:
        c = socket.create_connection(('169.254.169.254', 80), timeout)
        c.close()
    except socket.error:
        return

    return boto.utils.get_instance_metadata()


def _get_iam_role(get_ec2_metadata):
    """
    Fetches the first IAM role present in our local EC2 instance
    metadata, if available. Else returns None.
    """

    ec2_metadata = get_ec2_metadata()
    if ec2_metadata is None:
        return

    creds = ec2_metadata['iam']['security-credentials'].values()
    if len(creds) == 0:
        return
    cred = creds[0] # blindly take the first cred we find...
    if isinstance(cred, list):
        # earlier versions of boto (including 2.2.2) don't parse the cred
        # but return a list of JSON fragments.
        cred = json.loads(''.join(cred))
    return cred['AccessKeyId'], cred['SecretAccessKey'], cred['Token']



def _get_s3_endpoint(os_environ, get_ec2_metadata):
    """
    Returns the S3 endpoint to use when connecting to S3, using
    $AWS_DEFAULT_REGION if available, else the EC2 region, else the
    default region (us-east-1).
    """
    region_name = None
    if os_environ.get('AWS_DEFAULT_REGION'):
        region_name = os_environ['AWS_DEFAULT_REGION']
    else:
        ec2_metadata = get_ec2_metadata()
        if ec2_metadata is not None:
            region_name = ec2_metadata['placement']['availability-zone'][:-1]

    if region_name is None:
        return 's3.amazonaws.com'
    return 's3-{}.amazonaws.com'.format(region_name)


def _connect_s3(os_environ=None, get_ec2_metadata=None, boto_connect_s3=None):
    """
    Returns a boto S3Connection to the appropriate region, using
    either credentials in the standard environment variable or else, if
    called from an EC2 instance, in the first IAM role found in
    the instance metadata.
    """

    # Dependencies
    if os_environ is None:
        os_environ = os.environ
    if get_ec2_metadata is None:
        get_ec2_metadata = _get_ec2_metadata
    if boto_connect_s3 is None:
        boto_connect_s3 = boto.connect_s3

    s3_endpoint = _get_s3_endpoint(os_environ, get_ec2_metadata)
    try:
        logging.debug('s3_uploader._connect_s3: connecting to %s',
            s3_endpoint
            )
        return boto_connect_s3(host=s3_endpoint)
    except boto.exception.NoAuthHandlerFound:
        tup = _get_iam_role(get_ec2_metadata)
        if tup is None:
            raise Exception('No credentials found for connecting to S3')

        aws_access_key_id, aws_secret_access_key, security_token = tup
        logging.debug('s3_uploader._connect_s3: connecting to %s with IAM role',
            s3_endpoint
            )
        return boto_connect_s3(
            aws_access_key_id,
            aws_secret_access_key,
            security_token=security_token,
            host=s3_endpoint,
            )


#
# Path helpers
#

class LogfileUriFormatter(string.Formatter):

    mandatory_args = set(['prefix', 'filename', 'year', 'month', 'day'])

    # It actually seems like you will *always* want the fields above, simply
    # because listing logfiles is going to be really slow if you haven't
    # partitioned them by (year, month, day).
    #
    # So, leave out "expected_args"
    #
    # expected_args = mandatory_args.union(set(['year', 'month', 'day']))

    def check_unused_args(self, used_args, args, kwargs):
        missing_args = self.mandatory_args - used_args
        if missing_args:
            raise ValueError('upload_uri lacks mandatory fields: {}'.format(
                ', '.join(sorted(missing_args))
                ))

        # missing_args = self.expected_args - used_args
        # if missing_args:
        #     logging.warning(
        #         'upload_uri is missing these recommended fields: %s',
        #         ', '.join(sorted(missing_args))
        #         )



LOG_URI_FORMATTER = LogfileUriFormatter()

def get_logfile_uri(upload_uri, logfile):
    """
    Takes an upload_uri, such as:

        s3://nt8.logs.us-west-2/{prefix}/{year}/{month}/{day}/{filename}'

    and a LogFile. Returns the corresponding URL as a string, such as:

        s3://nt8.logs.us-west-2/haproxy/2013/07/27/haproxy-20130727T0100Z-i-34aea3fe.log.gz
    """
    return LOG_URI_FORMATTER.format(upload_uri,
        prefix=logfile.prefix,
        year=logfile.timestamp.year,
        month='{:02d}'.format(logfile.timestamp.month),
        day='{:02d}'.format(logfile.timestamp.day),
        hour='{:02d}'.format(logfile.timestamp.hour),
        minute='{:02d}'.format(logfile.timestamp.minute),
        filename=logfile.filename
        )


def get_parent_dir_uris(logfile_uris):
    """
    Takes an iterable of logfile_uri's. Returns back a set of all those URI's
    parent directories.
    """

    return set(
        logfile_uri.rsplit('/', 1)[0] + '/'
        for logfile_uri in logfile_uris
        )


class S3Uploader(BaseUploader):

    def __init__(self, upload_uri, connect_s3=None, storage_uri_for_key=None):
        """
        Takes an upload_uri, and two optional arguments for dependency
        injection during test runs:

            - connect_s3: a suitable implementation of _connect_s3()
            - storage_uri_for_key: a suitable implementation of
                                   boto.storage_uri_for_key()
        """
        self.upload_uri = upload_uri

        if connect_s3 is None:
            self.connect_s3 = _connect_s3
        else:
            self.connect_s3 = connect_s3

        if storage_uri_for_key is None:
            self.storage_uri_for_key = boto.storage_uri_for_key
        else:
            self.storage_uri_for_key = storage_uri_for_key

    def connect(self):
        self.s3_conn = self.connect_s3()
        self.bucket_cache = {}


    def _get_bucket(self, bucket_name):
        logging.debug('s3_uploader.S3Uploader._get_bucket: %s', bucket_name)
        bucket = self.bucket_cache.get(bucket_name)
        if bucket is None:
            bucket = self.s3_conn.get_bucket(bucket_name)
            self.bucket_cache[bucket_name] = bucket
        return bucket


    def check_uri(self):
        """
        Attempts to connect to S3 using our upload_uri.

        Returns a string if any error occurred.
        """
        try:
            logfile_uri = get_logfile_uri(self.upload_uri, parse.SAMPLE_LOGFILE)
        except ValueError, e:
            return str(e)

        u = boto.storage_uri(logfile_uri)

        try:
            self.s3_conn.get_bucket(u.bucket_name)
        except boto.exception.S3ResponseError:
            return 'Failed to find bucket {}'.format(u.bucket_name)



    def scan_remote(self, logfiles):
        """
        Takes a list of LogFile's. Returns back as two lists of
        LogFiles: those that have been uploaded already, and those that
        have not.
        """

        uploaded_set = set()
        logfile_uris = dict(
            (get_logfile_uri(self.upload_uri, logfile), logfile)
            for logfile in logfiles
            )

        parent_dir_uris = get_parent_dir_uris(logfile_uris)
        for uri in parent_dir_uris:
            u = boto.storage_uri(uri)
            bucket = self._get_bucket(u.bucket_name)
            logging.debug('S3Uploader.scan_remote: listing %r', uri)
            for key in bucket.list(prefix=u.object_name):
                logfile_uri = str(self.storage_uri_for_key(key))
                logging.debug('found %r', logfile_uri)
                logfile = logfile_uris.get(logfile_uri)
                if logfile is not None:
                    logging.debug('added to uploaded_set')
                    uploaded_set.add(logfile)

        not_uploaded_set = set(logfiles) - uploaded_set

        return uploaded_set, not_uploaded_set


    def upload_logfile(self, log_archive_dir, logfile):
        """
        Takes the path to the log archive directory and a LogFile
        corresponding to a file therein. Uploads the file, returning the
        file's URI.
        """

        logfile_uri = get_logfile_uri(self.upload_uri, logfile)
        u = boto.storage_uri(logfile_uri)
        bucket = self._get_bucket(u.bucket_name)
        key = bucket.get_key(u.object_name)
        if key is not None:
            logging.warning(
                'S3Uploader.upload_logfile: %s has already been uploaded',
                logfile_uri)
            return
        key = bucket.new_key(u.object_name)
        try:
            key.set_contents_from_filename(
                os.path.join(log_archive_dir, logfile.filename)
                )
        except boto.exception.BotoServerError, e:
            return e
