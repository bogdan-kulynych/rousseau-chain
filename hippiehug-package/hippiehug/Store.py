from .Utils import check_hash, ascii_hash


class DictionaryStore(object):
    def __init__(self, backend):
        self._backend = backend

    def __contains__(self, obj_hash):
        return self._backend.get(obj_hash, check_integrity=False) is not None

    def get(self, obj_hash, check_integrity=True):
        serialized_obj = self._backend.get(obj_hash)
        if serialized_obj is not None and check_integrity:
            check_hash(obj_hash, serialized_obj)
        return serialized_obj

    def put(self, serialized_obj):
        obj_hash = ascii_hash(serialized_obj)
        self._backend[obj_hash] = serialized_obj
