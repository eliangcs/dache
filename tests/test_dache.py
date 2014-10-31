import os
import shutil
import tempfile
import unittest

import dache


class TestLocMemCache(unittest.TestCase):

    CACHE_URL = 'locmem://'

    def setUp(self):
        self.cache = dache.Cache(self.CACHE_URL)

    def tearDown(self):
        self.cache.clear()
        self.cache.close()

    def test_simple(self):
        self.cache.set('key', 'value')
        self.assertEqual(self.cache.get('key'), 'value')

    def test_integer(self):
        self.cache.set('key', 123)
        self.assertEqual(self.cache.get('key'), 123)


class TestFileBasedCache(TestLocMemCache):

    CACHE_URL = 'file://%s' % tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        super(TestFileBasedCache, cls).tearDownClass()

        # Delete cache directory
        if os.path.exists(cls.CACHE_URL):
            shutil.rmtree(cls.CACHE_URL)


class TestRedisCache(TestLocMemCache):
    CACHE_URL = 'redis://127.0.0.1/0'
