try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import codecs
import os
import re
import sys


here = os.path.abspath(os.path.dirname(__file__))

py3 = sys.version_info[0] == 3


# Read the version number from a source file.
# Why read it, and not import?
# see https://groups.google.com/d/topic/pypa-dev/0PkjVpcxTzQ/discussion
def find_version(*file_paths):
    # Open in Latin-1 so that we avoid encoding errors.
    # Use codecs.open for Python 2 compatibility
    with codecs.open(os.path.join(here, *file_paths), 'r', 'latin1') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string')


def read_description(filename):
    with codecs.open(filename, encoding='utf-8') as f:
        return f.read()


basic_requires = [
    'six>=1.8.0,<1.9.0',
]

if py3:
    # python-memcached has another package for Python 3
    memcached_requires = [
        'python3-memcached>=1.51',
    ]

    # XXX: pylibmc hasn't officially supported Python 3, use development
    # version
    pylibmc_requires = [
        'https://github.com/lericson/pylibmc/archive/master.zip',
    ]
else:
    memcached_requires = [
        'python-memcached>=1.53',
    ]

    pylibmc_requires = [
        'pylibmc>=1.3.0,<1.4.0',
    ]

redis_requires = [
    'hiredis>=0.1.5,<0.2.0',
    'redis>=2.10.3,<2.11.0',
]

test_requires = [
    'pytest',
    'pytest-cov',
] + redis_requires + memcached_requires + pylibmc_requires

setup(
    name='Dache',
    version=find_version('dache', '__init__.py'),
    url='https://github.com/eliangcs/dache',
    description='Unify API for various cache backends',
    long_description=read_description('README.rst'),
    author='Chang-Hung Liang',
    author_email='eliang.cs@gmail.com',
    license='BSD',
    packages=['dache'],
    install_requires=basic_requires,
    extras_require={
        'memcached': memcached_requires,
        'pylibmc': pylibmc_requires,
        'redis': redis_requires,
        'test': test_requires,
    },
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ]
)
