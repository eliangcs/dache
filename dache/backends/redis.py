from __future__ import absolute_import

import redis
import time

from six.moves import cPickle as pickle

from .base import BaseCache, DEFAULT_TIMEOUT


DEFAULT_PORT = 6379


class RedisCache(BaseCache):

    def __init__(self, url, **options):
        super(RedisCache, self).__init__(**options)

        port = url.port or DEFAULT_PORT
        db = int(url.path[1:] or 0)
        self.redis = redis.StrictRedis(host=url.hostname, port=port, db=db,
                                       password=url.password)

    def get(self, key, default=None, version=None):
        key = self._get_redis_key(key, version)

        wrapper = self.redis.get(key)
        if not wrapper:
            return default

        wrapper = pickle.loads(wrapper)
        timeout = wrapper['timeout']
        if timeout is not None and timeout < time.time():
            self.redis.delete(key)
            return default

        return wrapper['value']

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self._get_redis_key(key, version)

        wrapper = {
            'timeout': self.get_backend_timeout(timeout),
            'value': value
        }
        self.redis.set(key, pickle.dumps(wrapper))

    def delete(self, key, version=None):
        key = self._get_redis_key(key, version)
        self.redis.delete(key)

    def clear(self):
        self.redis.flushdb()

    def _delete(self, redis_key):
        self.redis.delete(redis_key)

    def _get_redis_key(self, key, version=None):
        key = self.make_key(key, version)
        self.validate_key(key)
        return key
