======
Logjam
======

logjam 0.0.4

Released: 14-May-2015

.. image:: https://travis-ci.org/hblanks/logjam.png?branch=develop
        :target: https://travis-ci.org/hblanks/logjam


Logjam handles the (relatively) simple problem of compressing and archiving
ISO8601 logfiles. It works like this:

    1. Write all your logs into hourly files with an ISO8601 in their filenames.
    2. Run **logjam-compress** and **logjam-upload** on your log directories,
       either via cron or as a daemon.
    3. Your completed logfiles will automatically be compressed and uploaded to S3.

How to use it
-------------

the logfile format
~~~~~~~~~~~~~~~~~~

Your logfiles need two things:

	#. They must contain an **UTC ISO8601 timestamp**. In
	   ``haproxy-20130704T0000Z-us-west-2-i-ae23fega.log``, for instance,
	   the timestamp is ``20130704T0000Z``.
	#. They need to be written hourly or more frequent than hourly. Not daily.

If you use *rsyslog* or *syslog-ng*, then chances are you already use hourly
files. If not, they're very easy to configure.

logjam-compress
~~~~~~~~~~~~~~~

Sample entry in ``/etc/cron.d/``::

	10 * * * * * root logjam-compress --once /var/log/my-log-dir/

Sample command to put in an **upstart** config file or **runit** run script::

	logjam-compress /var/log/my-log-dir


logjam-upload
~~~~~~~~~~~~~~~

Sample entry in ``/etc/cron.d/``::

	10 * * * * * root logjam-upload --once /var/log/my-log-dir/archive/ s3://YOUR_BUCKET/{prefix}/{year}/{month}{/{day}/{filename}

Sample command to put in an **upstart** config file or **runit** run script::

	 logjam-upload /var/log/my-log-dir/archive/ s3://YOUR_BUCKET/{prefix}/{year}/{month}{/{day}/{filename}

**A note on authentication** ``logjam-upload`` looks for the standard boto
environment variables **$AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY**, plus
**$AWS_DEFAULT_REGION** to figure out which S3 region to use, and what creds
to use when connecting.

If those variables are not present, and you happen to be running
``logjam-upload`` from an instance with an IAM role, ``logjam-upload``
will parse its AWS credential from that, and connect to the local S3
region unless told otherwise with **$AWS_DEFAULT_REGION**.


What you need to get started
----------------------------

Just boto, and a bucket in S3.


Why is this useful?
-------------------

You may be right to think that don't need this, because if you have any
significant amount of logs, you're going to want some sort of online log
aggregation system, such as Logstash.

And if you have a really significant amount of logs, you're going to want
a really robust, distributed system for collecting *and* storing logs, such as
Scribe.

If you're big enough to need the latter, then this is not the tool for you.

But, if you're small enough that don't want to maintain your own distributed
system for storing logs, then you have two choices:

    1. Implement the online log aggregation solution, and then deal with log
       persistence, probably by setting up some sort S3 output to run on
       your log aggregation server. This will work, although you will have a
       SPOF where you can lose all your logs for a given time period.

    2. Implement a log persistence solution whose primary machinism is
       decoupled from your log aggregation solution.

In the second case, you no longer have a SPOF that can lose all your logs for a
given time, although when you lose an individual server, you *will lose its logs
from its last, partial hour.*

I've been very happy with the second case, and indeed when I have to
choose which to have first, I always choose persistence over
aggregation. Unfortunately, I'm always writing code to take care of the
persistence -- i.e. of compressing and uploading logfiles. So, finally,
here's a small, open source tool for it.

Running tests
-------------

Unit tests run with::

    python setup.py test

Integration tests run with:

    export SENTRY_DSN="https:// SOME SENTRY DSN"
    python tests/integration/test_all.py

Or run them all with:

    ./test_all.sh
