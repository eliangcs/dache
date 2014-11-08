"""LevelDB cache backend."""

from __future__ import absolute_import

import errno
import leveldb
import os
import random
import shutil
import time

from six.moves import cPickle as pickle

from .base import BaseCache, DEFAULT_TIMEOUT
from dache.utils.encoding import force_bytes


class LevelDBCache(BaseCache):

    # Maintain singleton LevelDB instances. Keys are directory paths and values
    # are LevelDB instances. LevelDB doesn't support multiprocessing and every
    # process can only have one connection at a time.
    _dbs = {}

    def __init__(self, url, **options):
        super(LevelDBCache, self).__init__(**options)

        self._dir = os.path.abspath(url.path)

    def get(self, key, default=None, version=None):
        key = self._make_and_validate_key(key, version)
        try:
            wrapper = self._db.Get(key)
        except KeyError:
            return default

        wrapper = pickle.loads(wrapper)
        timeout = wrapper['timeout']
        if timeout is not None and timeout < time.time():
            self._db.Delete(key)
            return default

        return wrapper['value']

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self._make_and_validate_key(key, version)
        self._cull()  # Make room if necessary
        wrapper = {
            'timeout': self.get_backend_timeout(timeout),
            'value': value
        }
        self._db.Put(key, pickle.dumps(wrapper))

    def delete(self, key, version=None):
        key = self._make_and_validate_key(key, version)
        self._db.Delete(key)

    def clear(self):
        if os.path.exists(self._dir):
            # Delete LevelDB directory
            shutil.rmtree(self._dir)

        # Remove the global reference to LevelDB instance so that it can be
        # re-created, otherwise the old keys will still be there even if files
        # no longer exist
        self._dbs.pop(self._dir, None)

    @property
    def _db(self):
        created = self._createdir()
        if self._dir in self._dbs:
            if created:
                del self._dbs[self._dir]
                self._dbs[self._dir] = leveldb.LevelDB(self._dir)
        else:
            self._dbs[self._dir] = leveldb.LevelDB(self._dir)
        return self._dbs[self._dir]

    def _make_and_validate_key(self, key, version):
        key = self.make_key(key, version=version)
        self.validate_key(key)
        return force_bytes(key)

    def _cull(self):
        if self._max_entries is None:
            # No limit on number of _max_entries
            return

        keys = list(self._db.RangeIter(include_value=False))
        num_entries = len(keys)

        if num_entries < self._max_entries:
            return  # Return early if no culling is required
        if self._cull_frequency == 0:
            return self.clear()

        # Delete a random selection of entries
        keys = random.sample(keys, int(num_entries / self._cull_frequency))
        for key in keys:
            self._db.Delete(key)

    def _createdir(self):
        if not os.path.exists(self._dir):
            try:
                os.makedirs(self._dir, 0o700)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise EnvironmentError(
                        "Cache directory '%s' does not exist "
                        "and could not be created'" % self._dir)
            else:
                return True
        return False
