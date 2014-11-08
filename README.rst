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

Built-in backends:

+--------------+-----------------------------------------------+--------------------------------------------------+
| Backend      | Required Python Package(s)                    | URL                                              |
+==============+===============================================+==================================================+
| File         |                                               | ``file:///DIR_PATH``                             |
+--------------+-----------------------------------------------+--------------------------------------------------+
| LevelDB      | ``leveldb``                                   | ``leveldb:///DIR_PATH``                          |
+--------------+-----------------------------------------------+--------------------------------------------------+
| Local memory |                                               | ``locmem://``                                    |
+--------------+-----------------------------------------------+--------------------------------------------------+
| Memcached    | ``python-memcached`` or ``python3-memcached`` | ``memcached://HOST:PORT``                        |
|              | ``pylibmc``                                   | ``pylibmc://HOST:PORT``                          |
+--------------+-----------------------------------------------+--------------------------------------------------+
| Redis        | ``redis`` and ``hiredis``                     | ``redis:///HOST:PORT/DB``                        |
+--------------+-----------------------------------------------+--------------------------------------------------+

To register a custom backend, you can use ``register_backend()``::

    >>> import dache
    >>> dache.register_backend('awesome', 'my.backend.MyAwesomeCache')
