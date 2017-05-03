# -*- coding: utf-8 -*-

import six
import threading
import contextlib
from hashlib import sha256 as xhash

from binascii import hexlify


def _custom_class_repr(name):
    class _ReprMetaclass(type):
        def __str__(self):
            return name
    return _ReprMetaclass


class IntegrityValidationError(Exception):
    __metaclass__ = _custom_class_repr('IntegrityValidationError')


def binary_hash(item):
    """
    >>> binary_hash(b'value')[:4] == six.b('\xcdB@M')
    True
    """
    return xhash(item).digest()


def ascii_hash(item):
    """
    >>> ascii_hash(b'value')[:4] == six.u('cd42')
    True
    """
    return hexlify(binary_hash(item)).decode('utf-8')


def check_hash(hsh, item):
    """
    >>> a = six.u('Correct.')
    >>> b = six.u('Incorrect')
    >>> h = ascii_hash(a)
    >>> check_hash(h, a)
    >>> check_hash(h, b)
    Traceback (most recent call last):
    ...
    IntegrityValidationError: Object has wrong hash.

    """
    if hsh != ascii_hash(item):
        raise IntegrityValidationError('Object has wrong hash.')
