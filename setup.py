try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='logjam',
    version='0.1.0',
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
