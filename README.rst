Dache
=====

.. image:: https://badge.fury.io/py/dache.svg
    :target: http://badge.fury.io/py/dache

.. image:: https://travis-ci.org/eliangcs/dache.svg?branch=master
    :target: https://travis-ci.org/eliangcs/dache

.. image:: https://coveralls.io/repos/eliangcs/dache/badge.png?branch=master
    :target: https://coveralls.io/r/eliangcs/dache

Forked from Django's cache framework, Dache is a Python library that provides
a unified API across various cache backends.

**WARNING**: This package is still in development. **Do NOT use it in
production!**


Installation
------------
::

    pip install dache


Usage
-----
::

    >>> import dache
    >>> cache = dache.Cache('locmem://')
    >>> cache.set('key', {'value': 1234})
    >>> cache.get('key')
    {'value': 1234}
