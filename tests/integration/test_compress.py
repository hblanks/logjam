import contextlib
import datetime
import gzip
import os.path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

import logjam.compress

ONE_HOUR = datetime.timedelta(hours=1)

ISO_FORMAT = '%Y%m%dT%H%MZ'

@contextlib.contextmanager
def temporary_directory():
    tempdir = None
    try:
        tempdir = tempfile.mkdtemp()
        yield tempdir
    finally:
        if tempdir and os.path.isdir(tempdir):
            shutil.rmtree(tempdir)



def write_logfiles(temp_dir):
    utcnow = datetime.datetime.utcnow()
    fname_patterns = [
        'flask-requests-{iso8601}-us-west-2-i-ae23fega.log',
        'haproxy-{iso8601}.log',
        ]

    filenames = [
        fname_pat.format(
            iso8601=(utcnow - t_minus * ONE_HOUR).strftime(ISO_FORMAT)
            )
        for fname_pat in fname_patterns
        for t_minus in range(3)
        ]

    for filename in filenames:
        with open(os.path.join(temp_dir, filename), 'w') as f:
            f.write('logfile {}!\n'.format(filename))

    return sorted(filenames)


class TestLogjamCompress(unittest.TestCase):

    def test_run_logjam_compress_once(self):
        with temporary_directory() as tempdir:
            filenames = write_logfiles(tempdir)
            expected = [
                fn.filename + '.gz' for fn in
                logjam.compress.yield_old_logfiles(
                    filenames,
                    datetime.datetime.utcnow()
                    )
                ]
            archive_dir = os.path.join(tempdir, 'archive')

            subprocess.check_call(['scripts/logjam-compress', '--once', tempdir])
            actual = os.listdir(archive_dir)
            self.assertEqual(expected, actual)

            # check logfile contents
            for n, p in ((n, os.path.join(archive_dir, n)) for n in actual):
                with gzip.GzipFile(p, 'r') as f:
                    expected = 'logfile {}!\n'.format(n[:-3])
                    self.assertEqual(expected, f.read())


    def test_run_logjam_compress_forever(self):
        MAX_TIME = 5
        with temporary_directory() as tempdir:
            filenames = write_logfiles(tempdir)
            expected = [
                fn.filename + '.gz' for fn in
                logjam.compress.yield_old_logfiles(
                    filenames,
                    datetime.datetime.utcnow()
                    )
                ]

            archive_dir = os.path.join(tempdir, 'archive')
            start = time.time()
            p = subprocess.Popen(['scripts/logjam-compress', tempdir])

            # Wait until all files are compressed, or max time is elapsed
            while time.time() - start < MAX_TIME:
                if os.path.isdir(archive_dir):
                    actual = os.listdir(archive_dir)
                else:
                    actual = None
                if expected == actual:
                    break

                # Make sure we're still running, then sleep
                self.assertEqual(None, p.poll())
                time.sleep(0.1)

            # ... and we should still be running
            self.assertEqual(None, p.poll())

            # Next, we should respond to SIGTERM
            p.terminate()
            start = time.time()
            while time.time() - start < MAX_TIME:
                if p.poll() is not None:
                    break

            if p.poll() is not None:
                self.assertEqual(-signal.SIGTERM, p.returncode)
            else:
                p.kill()
                p.wait()
                assert False, 'Process had to be SIGKILL\'d'

            # and after all that, our expected & actual logfile names should
            # match up.
            self.assertEqual(expected, actual)

            # check logfile contents
            for n, p in ((n, os.path.join(archive_dir, n)) for n in actual):
                with gzip.GzipFile(p, 'r') as f:
                    expected = 'logfile {}!\n'.format(n[:-3])
                    self.assertEqual(expected, f.read())
