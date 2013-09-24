try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='logjam',
    description='Small tools for archiving ISO8601 logfiles',
    version='0.0.1',
    author='Hunter Blanks',
    author_email='hblanks@artifex.org',
    url='https://github.com/hblanks/logjam',
    packages=['logjam',],
    license='MIT License',
    scripts=['scripts/logjam-compress', 'scripts/logjam-upload'],
    long_description=open('README.rst').read(),
    test_suite='tests.unit',
    install_requires=[
        'boto>=2.2.2',
        ],
    tests_require=[
        'boto>=2.2.2',
        ],
)
