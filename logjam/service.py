""" Helpers for long-running services in logjam """

import time

MIN_SLEEP_TIME = 1

DEFAULT_INTERVAL = 60

def do_forever(do_func, interval_secs, *args, **kwargs):
    """
    Calls do_func(*args, **kwargs) every interval_secs, accounting
    for time elapsed while the function is running.

    In addition, this function will sleep a minimum of MIN_SLEEP_SECS
    between calls of do_func(), so that this service (which is always
    assumed to be a low-resource agent) never ends up with a rapidly
    iterating loop.
    """
    last_run = time.time()
    while True:
        do_func(*args, **kwargs)
        elapsed_time = time.time() - last_run
        sleep_time = max(interval_secs - elapsed_time, 1)
        time.sleep(sleep_time)
