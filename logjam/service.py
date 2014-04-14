""" Helpers for long-running services in logjam """

import contextlib
import logging
import os
import time

try:
    import raven
except ImportError:
    raven = None


MIN_SLEEP_TIME = 1
DEFAULT_INTERVAL = 60


def configure_logging(log_level):
    """
    Takes a log level such as 'info'.

    Configures basic logging to stderr with that level.
    """
    logging.basicConfig(level=getattr(logging, log_level.upper()))


@contextlib.contextmanager
def sentry_context(tags, sentry_dsn=None):
    """
    Context manager that catches exceptions and reports them to Sentry.
    """

    client = None
    if raven is not None:
        if sentry_dsn is None:
            sentry_dsn = os.environ.get('SENTRY_DSN')
        if sentry_dsn:
            client = raven.Client(sentry_dsn)

    try:
        yield
    except:
        if client is not None:
            logging.error(
                'Exception occurred. Reporting to sentry', exc_info=True
            )
            result = client.captureException(tags=tags)
            print 'RESULT', result
        raise



def do_once(do_func, *args, **kwargs):
    """
    Calls do_func(*args, **kwargs).

    This function will attempt to report exceptions using the
    sentry client library, raven, if it is available for import
    and if SENTRY_DSN is set in the environment.
    """
    with sentry_context({'logjam.service': do_func.__name__}):
        do_func(*args, **kwargs)


def do_forever(do_func, interval_secs, *args, **kwargs):
    """
    Calls do_func(*args, **kwargs) every interval_secs, accounting
    for time elapsed while the function is running.

    This function will attempt to report exceptions using the
    sentry client library, raven, if it is available for import
    and if SENTRY_DSN is set in the environment.

    In addition, this function will sleep a minimum of MIN_SLEEP_SECS
    between calls of do_func(), so that this service (which is always
    assumed to be a low-resource agent) never ends up with a rapidly
    iterating loop.
    """

    with sentry_context({'logjam': do_func.__name__}):
        while True:
            start = time.time()
            do_func(*args, **kwargs)
            elapsed_time = time.time() - start

            sleep_time = max(interval_secs - elapsed_time, MIN_SLEEP_TIME)
            logging.debug('service.do_forever: sleeping %.2f', sleep_time)
            time.sleep(sleep_time)
