"""
Base class for Uploaders.
"""

class BaseUploader(object):

    def __init__(self, upload_uri):
        self.upload_uri = upload_uri


    def check_uri(self):
        """
        Returns a message if our URI is invalid; else None.
        """
        raise NotImplementedError


    def connect(self):
        """
        Initializes a connection to the Uploader's URI. May be called
        more than once.
        """
        pass


    def scan_remote(self, logfiles):
        """
        Takes a list of LogFile's. Returns back as two lists of
        LogFiles: those that have been uploaded already, and those that
        have not.
        """
        raise NotImplementedError


    def upload_logfile(log_archive_dir, logfile):
        """
        Takes the path to the log archive directory and a LogFile
        corresponding to a file therein. Uploads the file, returning the
        file's URI.
        """
        raise NotImplementedError
