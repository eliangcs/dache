import six
import sys
import urlparse

from importlib import import_module

from .backends.filebased import FileBasedCache
from .backends.locmem import LocMemCache


__all__ = ('register_backend', 'Cache')


_BACKENDS = {
    'file': FileBasedCache,
    'locmem': LocMemCache
}


def register_backend(url_scheme, backend_class):
    _BACKENDS[url_scheme] = backend_class


def import_string(dotted_path):
    """Import a dotted module path and return the attribute/class designated by
    the last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "%s" does not define a "%s" attribute/class' % (
            dotted_path, class_name)
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])


class Cache(object):

    def __init__(self, url, prefix='', timeout=None):
        # Create cache backend
        result = urlparse.urlparse(url)
        backend_class = _BACKENDS[result.scheme]
        if isinstance(backend_class, basestring):
            backend_class = import_string(backend_class)

        self._backend = backend_class(prefix=prefix, timeout=timeout)

        public_methods = ('add', 'get', 'set', 'delete', 'get_many', 'has_key'
                          'incr', 'decr', 'set_many', 'delete_many', 'clear',
                          'validate_key', 'incr_version', 'decr_version',
                          'close')
        for method in public_methods:
            setattr(self, method, getattr(self._backend, method))
