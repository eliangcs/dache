import urlparse

from dache.backends.base import CacheKeyWarning  # noqa
from dache.backends.filebased import FileBasedCache
from dache.backends.locmem import LocMemCache
from dache.backends.redis import RedisCache
from dache.utils.module_loading import import_string


__version__ = '0.0.1'

__all__ = ('register_backend', 'Cache', 'CacheKeyWarning')


_BACKENDS = {
    'file': FileBasedCache,
    'locmem': LocMemCache,
    'redis': RedisCache,
}


def register_backend(url_scheme, backend_class):
    """Register a cache backend."""
    _BACKENDS[url_scheme] = backend_class


class Cache(object):

    def __init__(self, url, **options):
        # Create cache backend
        result = urlparse.urlparse(url)
        backend_class = _BACKENDS[result.scheme]
        if isinstance(backend_class, basestring):
            backend_class = import_string(backend_class)

        self._backend = backend_class(result, **options)

        public_methods = ('add', 'get', 'set', 'delete', 'get_many', 'has_key',
                          'incr', 'decr', 'set_many', 'delete_many', 'clear',
                          'validate_key', 'incr_version', 'decr_version',
                          'close', '__contains__')
        for method in public_methods:
            setattr(self, method, getattr(self._backend, method))
