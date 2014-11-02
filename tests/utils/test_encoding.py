# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import datetime
import unittest

from dache.utils.encoding import force_bytes


class TestEncodingUtils(unittest.TestCase):

    def test_force_bytes_exception(self):
        """Test that force_bytes knows how to convert to bytes an exception
        containing non-ASCII characters in its args.
        """
        error_msg = "This is an exception, voilà"
        exc = ValueError(error_msg)
        result = force_bytes(exc)
        self.assertEqual(result, error_msg.encode('utf-8'))

    def test_force_bytes_strings_only(self):
        today = datetime.date.today()
        self.assertEqual(force_bytes(today, strings_only=True), today)

    def test_force_bytes_to_utf8(self):
        s = '中文'.encode('utf-8')
        self.assertEqual(force_bytes(s), s)

    def test_force_bytes_to_big5(self):
        s_utf8 = '中文'.encode('utf-8')
        s_big5 = '中文'.encode('big5')
        self.assertEqual(force_bytes(s_utf8, encoding='big5'), s_big5)

    def test_force_bytes_from_bytearray(self):
        s = bytearray(b'test')
        self.assertEqual(force_bytes(s), b'test')
