#!/usr/bin/env python

import os
import os.path
import unittest
import sys

# Add our project directory to sys.path, and PYTHONPATH for child procs
sys.path.append(
    os.path.join(os.path.dirname(__file__), '..', '..')
    )
pythonpath = os.environ.get('PYTHONPATH')
if pythonpath:
    os.environ['PYTHONPATH'] += sys.path[-1]
else:
    os.environ['PYTHONPATH'] = sys.path[-1]


from test_compress import TestLogjamCompress

if __name__ == '__main__':
    unittest.main()
