from __future__ import absolute_import

import redis

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

        value = self.redis.get(key)
        if not value:
            return default

        value = pickle.loads(value)
        return value

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self._get_redis_key(key, version)

        value = pickle.dumps(value)
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        pipeline = self.redis.pipeline(transaction=True)
        pipeline.set(key, value)
        if timeout is not None:
            pipeline.expire(key, int(timeout))
        pipeline.execute()

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
