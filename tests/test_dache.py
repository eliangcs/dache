# -*- coding: utf-8 -*-

import os
import shutil
import tempfile
import time
import unittest
import warnings

import dache

from six.moves import cPickle as pickle

from dache import CacheKeyWarning


# The default server on which the cache services are installed
DEFAULT_CACHE_SERVER = '127.0.0.1'


# functions/classes for complex data type tests
def f():
    return 42


class C:
    def m(n):
        return 24


class Unpickable(object):
    def __getstate__(self):
        raise pickle.PickleError()


class TestLocMemCache(unittest.TestCase):

    CACHE_URL = 'locmem://'

    def setUp(self):
        self.cache = dache.Cache(self.CACHE_URL)

    def tearDown(self):
        self.cache.clear()
        self.cache.close()

    def test_simple(self):
        # Simple cache set/get works
        self.cache.set('key', 'value')
        self.assertEqual(self.cache.get('key'), 'value')

    def test_add(self):
        # A key can be added to a cache
        self.cache.add("addkey1", "value")
        result = self.cache.add("addkey1", "newvalue")
        self.assertFalse(result)
        self.assertEqual(self.cache.get("addkey1"), "value")

    def test_prefix(self):
        # Test for same cache key conflicts between shared backend
        self.cache.set('somekey', 'value')

        # should not be set in the prefixed cache
        another_cache = dache.Cache(self.CACHE_URL, key_prefix='prefix')
        self.assertFalse(another_cache.has_key('somekey'))  # noqa

        another_cache.set('somekey', 'value2')

        self.assertEqual(self.cache.get('somekey'), 'value')
        self.assertEqual(another_cache.get('somekey'), 'value2')

    def test_non_existent(self):
        # Non-existent cache keys return as None/default
        # get with non-existent keys
        self.assertIsNone(self.cache.get("does_not_exist"))
        self.assertEqual(self.cache.get("does_not_exist", "bang!"), "bang!")

    def test_get_many(self):
        # Multiple cache keys can be returned using get_many
        self.cache.set('a', 'a')
        self.cache.set('b', 'b')
        self.cache.set('c', 'c')
        self.cache.set('d', 'd')
        self.assertDictEqual(self.cache.get_many(['a', 'c', 'd']),
                             {'a': 'a', 'c': 'c', 'd': 'd'})
        self.assertDictEqual(self.cache.get_many(['a', 'b', 'e']),
                             {'a': 'a', 'b': 'b'})

    def test_delete(self):
        # Cache keys can be deleted
        self.cache.set("key1", "spam")
        self.cache.set("key2", "eggs")
        self.assertEqual(self.cache.get("key1"), "spam")
        self.cache.delete("key1")
        self.assertIsNone(self.cache.get("key1"))
        self.assertEqual(self.cache.get("key2"), "eggs")

    def test_has_key(self):
        # The cache can be inspected for cache keys
        self.cache.set("hello1", "goodbye1")
        self.assertTrue(self.cache.has_key("hello1"))  # noqa
        self.assertFalse(self.cache.has_key("goodbye1"))  # noqa
        self.cache.set("no_expiry", "here", None)
        self.assertTrue(self.cache.has_key("no_expiry"))  # noqa

    def test_in(self):
        # The in operator can be used to inspect cache contents
        self.cache.set("hello2", "goodbye2")
        self.assertIn("hello2", self.cache)
        self.assertNotIn("goodbye2", self.cache)

    def test_incr(self):
        # Cache values can be incremented
        self.cache.set('answer', 41)
        self.assertEqual(self.cache.incr('answer'), 42)
        self.assertEqual(self.cache.get('answer'), 42)
        self.assertEqual(self.cache.incr('answer', 10), 52)
        self.assertEqual(self.cache.get('answer'), 52)
        self.assertEqual(self.cache.incr('answer', -10), 42)
        self.assertRaises(ValueError, self.cache.incr, 'does_not_exist')

    def test_decr(self):
        # Cache values can be decremented
        self.cache.set('answer', 43)
        self.assertEqual(self.cache.decr('answer'), 42)
        self.assertEqual(self.cache.get('answer'), 42)
        self.assertEqual(self.cache.decr('answer', 10), 32)
        self.assertEqual(self.cache.get('answer'), 32)
        self.assertEqual(self.cache.decr('answer', -10), 42)
        self.assertRaises(ValueError, self.cache.decr, 'does_not_exist')

    def test_close(self):
        self.assertTrue(hasattr(self.cache, 'close'))
        self.cache.close()

    def test_data_types(self):
        # Many different data types can be cached
        stuff = {
            'string': 'this is a string',
            'int': 42,
            'list': [1, 2, 3, 4],
            'tuple': (1, 2, 3, 4),
            'dict': {'A': 1, 'B': 2},
            'function': f,
            'class': C,
        }
        self.cache.set("stuff", stuff)
        self.assertEqual(self.cache.get("stuff"), stuff)

    # TODO: More tests with object instances

    def test_expiration(self):
        # Cache values can be set to expire
        self.cache.set('expire1', 'very quickly', 1)
        self.cache.set('expire2', 'very quickly', 1)
        self.cache.set('expire3', 'very quickly', 1)

        time.sleep(2)
        self.assertIsNone(self.cache.get("expire1"))

        self.cache.add("expire2", "newvalue")
        self.assertEqual(self.cache.get("expire2"), "newvalue")
        self.assertFalse(self.cache.has_key("expire3"))  # noqa

    def test_unicode(self):
        # Unicode values can be cached
        stuff = {
            'ascii': 'ascii_value',
            'unicode_ascii': 'Iñtërnâtiônàlizætiøn1',
            'Iñtërnâtiônàlizætiøn': 'Iñtërnâtiônàlizætiøn2',
            'ascii2': {'x': 1}
        }
        # Test `set`
        for (key, value) in stuff.items():
            self.cache.set(key, value)
            self.assertEqual(self.cache.get(key), value)

        # Test `add`
        for (key, value) in stuff.items():
            self.cache.delete(key)
            self.cache.add(key, value)
            self.assertEqual(self.cache.get(key), value)

        # Test `set_many`
        for (key, value) in stuff.items():
            self.cache.delete(key)
        self.cache.set_many(stuff)
        for (key, value) in stuff.items():
            self.assertEqual(self.cache.get(key), value)

    def test_binary_string(self):
        # Binary strings should be cacheable
        from zlib import compress, decompress
        value = 'value_to_be_compressed'
        compressed_value = compress(value.encode())

        # Test set
        self.cache.set('binary1', compressed_value)
        compressed_result = self.cache.get('binary1')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, decompress(compressed_result).decode())

        # Test add
        self.cache.add('binary1-add', compressed_value)
        compressed_result = self.cache.get('binary1-add')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, decompress(compressed_result).decode())

        # Test set_many
        self.cache.set_many({'binary1-set_many': compressed_value})
        compressed_result = self.cache.get('binary1-set_many')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, decompress(compressed_result).decode())

    def test_set_many(self):
        # Multiple keys can be set using set_many
        self.cache.set_many({"key1": "spam", "key2": "eggs"})
        self.assertEqual(self.cache.get("key1"), "spam")
        self.assertEqual(self.cache.get("key2"), "eggs")

    def test_set_many_expiration(self):
        # set_many takes a second ``timeout`` parameter
        self.cache.set_many({"key1": "spam", "key2": "eggs"}, 1)
        time.sleep(2)
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))

    def test_delete_many(self):
        # Multiple keys can be deleted using delete_many
        self.cache.set("key1", "spam")
        self.cache.set("key2", "eggs")
        self.cache.set("key3", "ham")
        self.cache.delete_many(["key1", "key2"])
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))
        self.assertEqual(self.cache.get("key3"), "ham")

    def test_clear(self):
        # The cache can be emptied using clear
        self.cache.set("key1", "spam")
        self.cache.set("key2", "eggs")
        self.cache.clear()
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))

    def test_long_timeout(self):
        """Using a timeout greater than 30 days makes memcached think it is an
        absolute expiration timestamp instead of a relative offset. Test that
        we honour this convention. Refs Django #12399.
        """
        # 30 days + 1 second
        self.cache.set('key1', 'eggs', 60 * 60 * 24 * 30 + 1)
        self.assertEqual(self.cache.get('key1'), 'eggs')

        self.cache.add('key2', 'ham', 60 * 60 * 24 * 30 + 1)
        self.assertEqual(self.cache.get('key2'), 'ham')

        self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'},
                            60 * 60 * 24 * 30 + 1)
        self.assertEqual(self.cache.get('key3'), 'sausage')
        self.assertEqual(self.cache.get('key4'), 'lobster bisque')

    def test_forever_timeout(self):
        """Passing in None into timeout results in a value that is cached
        forever.
        """
        self.cache.set('key1', 'eggs', None)
        self.assertEqual(self.cache.get('key1'), 'eggs')

        self.cache.add('key2', 'ham', None)
        self.assertEqual(self.cache.get('key2'), 'ham')
        added = self.cache.add('key1', 'new eggs', None)
        self.assertEqual(added, False)
        self.assertEqual(self.cache.get('key1'), 'eggs')

        self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'},
                            None)
        self.assertEqual(self.cache.get('key3'), 'sausage')
        self.assertEqual(self.cache.get('key4'), 'lobster bisque')

    def test_zero_timeout(self):
        """Passing in zero into timeout results in a value that is not cached.
        """
        self.cache.set('key1', 'eggs', 0)
        self.assertIsNone(self.cache.get('key1'))

        self.cache.add('key2', 'ham', 0)
        self.assertIsNone(self.cache.get('key2'))

        self.cache.set_many({'key3': 'sausage', 'key4': 'lobster bisque'}, 0)
        self.assertIsNone(self.cache.get('key3'))
        self.assertIsNone(self.cache.get('key4'))

    def test_float_timeout(self):
        # Make sure a timeout given as a float doesn't crash anything.
        self.cache.set("key1", "spam", 100.2)
        self.assertEqual(self.cache.get("key1"), "spam")

    def _perform_cull_test(self, cull_cache, initial_count, final_count):
        # Create initial cache key entries. This will overflow the cache,
        # causing a cull.
        for i in range(1, initial_count):
            cull_cache.set('cull%d' % i, 'value', 1000)
        count = 0
        # Count how many keys are left in the self.cache.
        for i in range(1, initial_count):
            if cull_cache.has_key('cull%d' % i):  # noqa
                count = count + 1
        self.assertEqual(count, final_count)

    def test_cull(self):
        cache = dache.Cache(self.CACHE_URL, max_entries=30)
        self._perform_cull_test(cache, 50, 29)

    def test_zero_cull(self):
        cache = dache.Cache(self.CACHE_URL, max_entries=30, cull_frequency=0)
        self._perform_cull_test(cache, 50, 19)

    def test_unlimited_max_entries(self):
        # Passing None to max_entries should make it have unlimited number of
        # entries of capacity
        cache = dache.Cache(self.CACHE_URL, max_entries=None)

        # Can't really test unlimited number, so just test with a large number
        self._perform_cull_test(cache, 1000, 999)

    def test_invalid_keys(self):
        """All the builtin backends (except memcached, see below) should warn
        on keys that would be refused by memcached. This encourages portable
        caching code without making it too difficult to use production backends
        with more liberal key rules. Refs Django #6447.
        """
        # mimic custom ``make_key`` method being defined since the default will
        # never show the below warnings
        def func(key, *args):
            return key

        old_func = self.cache._backend.key_func
        self.cache._backend.key_func = func

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # memcached does not allow whitespace or control characters in
                # keys
                self.cache.set('key with spaces', 'value')
                self.assertEqual(len(w), 2)
                self.assertIsInstance(w[0].message, CacheKeyWarning)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # memcached limits key length to 250
                self.cache.set('a' * 251, 'value')
                self.assertEqual(len(w), 1)
                self.assertIsInstance(w[0].message, CacheKeyWarning)
        finally:
            self.cache._backend.key_func = old_func

    def test_cache_versioning_get_set(self):
        # set, using default version = 1
        self.cache.set('answer1', 42)
        self.assertEqual(self.cache.get('answer1'), 42)
        self.assertEqual(self.cache.get('answer1', version=1), 42)
        self.assertIsNone(self.cache.get('answer1', version=2))

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        self.assertIsNone(cache_v2.get('answer1'))
        self.assertEqual(cache_v2.get('answer1', version=1), 42)
        self.assertIsNone(cache_v2.get('answer1', version=2))

        # set, default version = 1, but manually override version = 2
        self.cache.set('answer2', 42, version=2)
        self.assertIsNone(self.cache.get('answer2'))
        self.assertIsNone(self.cache.get('answer2', version=1))
        self.assertEqual(self.cache.get('answer2', version=2), 42)

        self.assertEqual(cache_v2.get('answer2'), 42)
        self.assertIsNone(cache_v2.get('answer2', version=1))
        self.assertEqual(cache_v2.get('answer2', version=2), 42)

        # v2 set, using default version = 2
        cache_v2.set('answer3', 42)
        self.assertIsNone(self.cache.get('answer3'))
        self.assertIsNone(self.cache.get('answer3', version=1))
        self.assertEqual(self.cache.get('answer3', version=2), 42)

        self.assertEqual(cache_v2.get('answer3'), 42)
        self.assertIsNone(cache_v2.get('answer3', version=1))
        self.assertEqual(cache_v2.get('answer3', version=2), 42)

        # v2 set, default version = 2, but manually override version = 1
        cache_v2.set('answer4', 42, version=1)
        self.assertEqual(self.cache.get('answer4'), 42)
        self.assertEqual(self.cache.get('answer4', version=1), 42)
        self.assertIsNone(self.cache.get('answer4', version=2))

        self.assertIsNone(cache_v2.get('answer4'))
        self.assertEqual(cache_v2.get('answer4', version=1), 42)
        self.assertIsNone(cache_v2.get('answer4', version=2))

    def test_cache_versioning_add(self):

        # add, default version = 1, but manually override version = 2
        self.cache.add('answer1', 42, version=2)
        self.assertIsNone(self.cache.get('answer1', version=1))
        self.assertEqual(self.cache.get('answer1', version=2), 42)

        self.cache.add('answer1', 37, version=2)
        self.assertIsNone(self.cache.get('answer1', version=1))
        self.assertEqual(self.cache.get('answer1', version=2), 42)

        self.cache.add('answer1', 37, version=1)
        self.assertEqual(self.cache.get('answer1', version=1), 37)
        self.assertEqual(self.cache.get('answer1', version=2), 42)

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        # v2 add, using default version = 2
        cache_v2.add('answer2', 42)
        self.assertIsNone(self.cache.get('answer2', version=1))
        self.assertEqual(self.cache.get('answer2', version=2), 42)

        cache_v2.add('answer2', 37)
        self.assertIsNone(self.cache.get('answer2', version=1))
        self.assertEqual(self.cache.get('answer2', version=2), 42)

        cache_v2.add('answer2', 37, version=1)
        self.assertEqual(self.cache.get('answer2', version=1), 37)
        self.assertEqual(self.cache.get('answer2', version=2), 42)

        # v2 add, default version = 2, but manually override version = 1
        cache_v2.add('answer3', 42, version=1)
        self.assertEqual(self.cache.get('answer3', version=1), 42)
        self.assertIsNone(self.cache.get('answer3', version=2))

        cache_v2.add('answer3', 37, version=1)
        self.assertEqual(self.cache.get('answer3', version=1), 42)
        self.assertIsNone(self.cache.get('answer3', version=2))

        cache_v2.add('answer3', 37)
        self.assertEqual(self.cache.get('answer3', version=1), 42)
        self.assertEqual(self.cache.get('answer3', version=2), 37)

    def test_cache_versioning_has_key(self):
        self.cache.set('answer1', 42)

        # has_key
        self.assertTrue(self.cache.has_key('answer1'))  # noqa
        self.assertTrue(self.cache.has_key('answer1', version=1))  # noqa
        self.assertFalse(self.cache.has_key('answer1', version=2))  # noqa

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        self.assertFalse(cache_v2.has_key('answer1'))  # noqa
        self.assertTrue(cache_v2.has_key('answer1', version=1))  # noqa
        self.assertFalse(cache_v2.has_key('answer1', version=2))  # noqa

    def test_cache_versioning_delete(self):
        self.cache.set('answer1', 37, version=1)
        self.cache.set('answer1', 42, version=2)
        self.cache.delete('answer1')
        self.assertIsNone(self.cache.get('answer1', version=1))
        self.assertEqual(self.cache.get('answer1', version=2), 42)

        self.cache.set('answer2', 37, version=1)
        self.cache.set('answer2', 42, version=2)
        self.cache.delete('answer2', version=2)
        self.assertEqual(self.cache.get('answer2', version=1), 37)
        self.assertIsNone(self.cache.get('answer2', version=2))

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        self.cache.set('answer3', 37, version=1)
        self.cache.set('answer3', 42, version=2)
        cache_v2.delete('answer3')
        self.assertEqual(self.cache.get('answer3', version=1), 37)
        self.assertIsNone(self.cache.get('answer3', version=2))

        self.cache.set('answer4', 37, version=1)
        self.cache.set('answer4', 42, version=2)
        cache_v2.delete('answer4', version=1)
        self.assertIsNone(self.cache.get('answer4', version=1))
        self.assertEqual(self.cache.get('answer4', version=2), 42)

    def test_cache_versioning_incr_decr(self):
        self.cache.set('answer1', 37, version=1)
        self.cache.set('answer1', 42, version=2)
        self.cache.incr('answer1')
        self.assertEqual(self.cache.get('answer1', version=1), 38)
        self.assertEqual(self.cache.get('answer1', version=2), 42)
        self.cache.decr('answer1')
        self.assertEqual(self.cache.get('answer1', version=1), 37)
        self.assertEqual(self.cache.get('answer1', version=2), 42)

        self.cache.set('answer2', 37, version=1)
        self.cache.set('answer2', 42, version=2)
        self.cache.incr('answer2', version=2)
        self.assertEqual(self.cache.get('answer2', version=1), 37)
        self.assertEqual(self.cache.get('answer2', version=2), 43)
        self.cache.decr('answer2', version=2)
        self.assertEqual(self.cache.get('answer2', version=1), 37)
        self.assertEqual(self.cache.get('answer2', version=2), 42)

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        self.cache.set('answer3', 37, version=1)
        self.cache.set('answer3', 42, version=2)
        cache_v2.incr('answer3')
        self.assertEqual(self.cache.get('answer3', version=1), 37)
        self.assertEqual(self.cache.get('answer3', version=2), 43)
        cache_v2.decr('answer3')
        self.assertEqual(self.cache.get('answer3', version=1), 37)
        self.assertEqual(self.cache.get('answer3', version=2), 42)

        self.cache.set('answer4', 37, version=1)
        self.cache.set('answer4', 42, version=2)
        cache_v2.incr('answer4', version=1)
        self.assertEqual(self.cache.get('answer4', version=1), 38)
        self.assertEqual(self.cache.get('answer4', version=2), 42)
        cache_v2.decr('answer4', version=1)
        self.assertEqual(self.cache.get('answer4', version=1), 37)
        self.assertEqual(self.cache.get('answer4', version=2), 42)

    def test_cache_versioning_get_set_many(self):
        # set, using default version = 1
        self.cache.set_many({'ford1': 37, 'arthur1': 42})
        self.assertDictEqual(self.cache.get_many(['ford1', 'arthur1']),
                             {'ford1': 37, 'arthur1': 42})
        self.assertDictEqual(
            self.cache.get_many(['ford1', 'arthur1'], version=1),
            {'ford1': 37, 'arthur1': 42})
        self.assertDictEqual(
            self.cache.get_many(['ford1', 'arthur1'], version=2), {})

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        self.assertDictEqual(cache_v2.get_many(['ford1', 'arthur1']), {})
        self.assertDictEqual(
            cache_v2.get_many(['ford1', 'arthur1'], version=1),
            {'ford1': 37, 'arthur1': 42})
        self.assertDictEqual(
            cache_v2.get_many(['ford1', 'arthur1'], version=2), {})

        # set, default version = 1, but manually override version = 2
        self.cache.set_many({'ford2': 37, 'arthur2': 42}, version=2)
        self.assertDictEqual(self.cache.get_many(['ford2', 'arthur2']), {})
        self.assertDictEqual(
            self.cache.get_many(['ford2', 'arthur2'], version=1), {})
        self.assertDictEqual(
            self.cache.get_many(['ford2', 'arthur2'], version=2),
            {'ford2': 37, 'arthur2': 42})

        self.assertDictEqual(cache_v2.get_many(['ford2', 'arthur2']),
                             {'ford2': 37, 'arthur2': 42})
        self.assertDictEqual(
            cache_v2.get_many(['ford2', 'arthur2'], version=1), {})
        self.assertDictEqual(
            cache_v2.get_many(['ford2', 'arthur2'], version=2),
            {'ford2': 37, 'arthur2': 42})

        # v2 set, using default version = 2
        cache_v2.set_many({'ford3': 37, 'arthur3': 42})
        self.assertDictEqual(self.cache.get_many(['ford3', 'arthur3']), {})
        self.assertDictEqual(
            self.cache.get_many(['ford3', 'arthur3'], version=1), {})
        self.assertDictEqual(
            self.cache.get_many(['ford3', 'arthur3'], version=2),
            {'ford3': 37, 'arthur3': 42})

        self.assertDictEqual(cache_v2.get_many(['ford3', 'arthur3']),
                             {'ford3': 37, 'arthur3': 42})
        self.assertDictEqual(
            cache_v2.get_many(['ford3', 'arthur3'], version=1), {})
        self.assertDictEqual(
            cache_v2.get_many(['ford3', 'arthur3'], version=2),
            {'ford3': 37, 'arthur3': 42})

        # v2 set, default version = 2, but manually override version = 1
        cache_v2.set_many({'ford4': 37, 'arthur4': 42}, version=1)
        self.assertDictEqual(self.cache.get_many(['ford4', 'arthur4']),
                             {'ford4': 37, 'arthur4': 42})
        self.assertDictEqual(
            self.cache.get_many(['ford4', 'arthur4'], version=1),
            {'ford4': 37, 'arthur4': 42})
        self.assertDictEqual(
            self.cache.get_many(['ford4', 'arthur4'], version=2), {})

        self.assertDictEqual(cache_v2.get_many(['ford4', 'arthur4']), {})
        self.assertDictEqual(
            cache_v2.get_many(['ford4', 'arthur4'], version=1),
            {'ford4': 37, 'arthur4': 42})
        self.assertDictEqual(
            cache_v2.get_many(['ford4', 'arthur4'], version=2), {})

    def test_incr_version(self):
        self.cache.set('answer', 42, version=2)
        self.assertIsNone(self.cache.get('answer'))
        self.assertIsNone(self.cache.get('answer', version=1))
        self.assertEqual(self.cache.get('answer', version=2), 42)
        self.assertIsNone(self.cache.get('answer', version=3))

        self.assertEqual(self.cache.incr_version('answer', version=2), 3)
        self.assertIsNone(self.cache.get('answer'))
        self.assertIsNone(self.cache.get('answer', version=1))
        self.assertIsNone(self.cache.get('answer', version=2))
        self.assertEqual(self.cache.get('answer', version=3), 42)

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        cache_v2.set('answer2', 42)
        self.assertEqual(cache_v2.get('answer2'), 42)
        self.assertIsNone(cache_v2.get('answer2', version=1))
        self.assertEqual(cache_v2.get('answer2', version=2), 42)
        self.assertIsNone(cache_v2.get('answer2', version=3))

        self.assertEqual(cache_v2.incr_version('answer2'), 3)
        self.assertIsNone(cache_v2.get('answer2'))
        self.assertIsNone(cache_v2.get('answer2', version=1))
        self.assertIsNone(cache_v2.get('answer2', version=2))
        self.assertEqual(cache_v2.get('answer2', version=3), 42)

        self.assertRaises(ValueError, self.cache.incr_version,
                          'does_not_exist')

    def test_decr_version(self):
        self.cache.set('answer', 42, version=2)
        self.assertIsNone(self.cache.get('answer'))
        self.assertIsNone(self.cache.get('answer', version=1))
        self.assertEqual(self.cache.get('answer', version=2), 42)

        self.assertEqual(self.cache.decr_version('answer', version=2), 1)
        self.assertEqual(self.cache.get('answer'), 42)
        self.assertEqual(self.cache.get('answer', version=1), 42)
        self.assertIsNone(self.cache.get('answer', version=2))

        cache_v2 = dache.Cache(self.CACHE_URL, version=2)

        cache_v2.set('answer2', 42)
        self.assertEqual(cache_v2.get('answer2'), 42)
        self.assertIsNone(cache_v2.get('answer2', version=1))
        self.assertEqual(cache_v2.get('answer2', version=2), 42)

        self.assertEqual(cache_v2.decr_version('answer2'), 1)
        self.assertIsNone(cache_v2.get('answer2'))
        self.assertEqual(cache_v2.get('answer2', version=1), 42)
        self.assertIsNone(cache_v2.get('answer2', version=2))

        self.assertRaises(ValueError, self.cache.decr_version,
                          'does_not_exist', version=2)

    def test_custom_key_func(self):
        # Two caches with different key functions aren't visible to each other
        self.cache.set('answer1', 42)
        self.assertEqual(self.cache.get('answer1'), 42)

        def custom_key_func(key, key_prefix, version):
            """A customized cache key function."""
            return 'CUSTOM-' + '-'.join([key_prefix, str(version), key])

        cache_custom_key = dache.Cache(self.CACHE_URL,
                                       key_func=custom_key_func)
        self.assertIsNone(cache_custom_key.get('answer1'))

        cache_custom_key.set('answer2', 42)
        self.assertIsNone(self.cache.get('answer2'))
        self.assertEqual(cache_custom_key.get('answer2'), 42)

    def test_add_fail_on_pickleerror(self):
        # See https://code.djangoproject.com/ticket/21200
        with self.assertRaises(pickle.PickleError):
            self.cache.add('unpickable', Unpickable())

    def test_set_fail_on_pickleerror(self):
        # See https://code.djangoproject.com/ticket/21200
        with self.assertRaises(pickle.PickleError):
            self.cache.set('unpickable', Unpickable())


class TestFileBasedCache(TestLocMemCache):

    CACHE_URL = 'file://%s' % tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        super(TestFileBasedCache, cls).tearDownClass()

        # Delete cache directory
        if os.path.exists(cls.CACHE_URL):
            shutil.rmtree(cls.CACHE_URL)


class DontTestCullMixin(object):
    """Some backends support culling natively, so no need to implement nor test
    cullling."""
    def test_cull(self):
        # Culling isn't implemented for the backend
        pass

    def test_zero_cull(self):
        # Culling isn't implemented for the backend
        pass


class TestRedisCache(DontTestCullMixin, TestLocMemCache):
    CACHE_URL = 'redis://%s/0' % os.getenv('CACHE_SERVER',
                                           DEFAULT_CACHE_SERVER)


class TestMemcachedCache(DontTestCullMixin, TestLocMemCache):

    CACHE_URL = 'memcached://%s' % os.getenv('CACHE_SERVER',
                                             DEFAULT_CACHE_SERVER)

    def test_invalid_keys(self):
        """On memcached, we don't introduce a duplicate key validation step
        (for speed reasons), we just let the memcached API library raise its
        own exception on bad keys. Refs Django #6447. In order to be
        memcached-API-library agnostic, we only assert that a generic exception
        of some kind is raised.
        """
        # memcached does not allow whitespace or control characters in keys
        with self.assertRaises(Exception):
            self.cache.set('key with spaces', 'value')

        # memcached limits key length to 250
        with self.assertRaises(Exception):
            self.cache.set('a' * 251, 'value')


class TestPyLibMCCache(TestMemcachedCache):
    CACHE_URL = 'pylibmc://%s' % os.getenv('CACHE_SERVER',
                                           DEFAULT_CACHE_SERVER)
