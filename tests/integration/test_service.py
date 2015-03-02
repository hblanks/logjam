import os
import contextlib
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import logjam.service


@contextlib.contextmanager
def environ_var(name, value):
    """ Temporarily sets some environment variable name=value. """
    orig_value = os.environ.get(name)
    try:
        os.environ[name] = value
        yield
    finally:
        if orig_value is None:
            del os.environ[name]
        else:
            os.environ[name] = orig_value

class TestLogjamServiceError(Exception):
    pass



class TestLogjamService(unittest.TestCase):

    def test_sentry_reporting(self):
        import raven

        def fail():
            raise TestLogjamServiceError

        with self.assertRaises(TestLogjamServiceError):
            with environ_var('SENTRY_DSN', 'http://foo:gar@localhost:9999/1'):
                logjam.service.do_once(fail)


if __name__ == '__main__':
    unittest.main()