try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='logjam',
    version='0.0.4-beta',

    # metadata
    author_email='hblanks@artifex.org',
    author='Hunter Blanks',
    description='Small tools for archiving ISO8601 logfiles',
    license='MIT License',
    long_description=open('README.rst').read(),
    url='https://github.com/hblanks/logjam',

    # build instructions
    install_requires=[
        'boto>=2.2.2',
        ],
    packages=['logjam',],
    scripts=['scripts/logjam-compress', 'scripts/logjam-upload'],
    test_suite='tests.unit',
    tests_require=[
        'boto>=2.2.2',
        ],
)
