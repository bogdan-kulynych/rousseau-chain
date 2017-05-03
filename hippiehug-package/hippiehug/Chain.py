from collections import OrderedDict

from .Utils import ascii_hash


class BaseBlock(object):
    """
    Abstract skip-list base block.

    Requires that serialisation is defined in concretizations.

    Example::

        class JsonBlock(BaseBlock):
            def serialize(self):
                return json.dumps(
                # ...
                # Things to be serialized
                # ...
                ).encode('utf-8')

            @staticmethod
            def deserialize(self, serialized):
                vals = json.loads(serialized)
                # ...
                # Do deserialization
                # ...
                return JsonBlock(**vals)

    .. seealso ..
    TODO: Serializable block classes

    """
    def __init__(self, index=0, payload=None, fingers=None, _chain=None):
        """
        :param index: Sequence index
        :param payload: Block payload
        :param fingers: Skip-list fingers (list of back-pointers to
                        previous blocks)
        :param _chain: Chain that the block belongs to

        """
        self.index = index
        self.payload = payload
        self.fingers = fingers or []
        self._chain = _chain

    @property
    def hid(self):
        """Return the hash of the block."""
        serialized = self.serialize()
        return ascii_hash(serialized)

    @staticmethod
    def skipchain_indices(index):
        """Finger indices for the current index."""
        return set(index - 1 - ((index - 1) % (2**f)) for f in range(64))

    @classmethod
    def make_next_block(cls, current_block, _chain=None, *args, **kwargs):
        """Build an empty subsequent block.

        This method prefills the index and fingers. Payload is expected
        to be added before commiting to the chain.
        """

        if current_block is None:
            return cls(fingers=None, _chain=_chain, *args, **kwargs)

        new_index = current_block.index + 1
        newfingers = [(current_block.index, current_block.hid)]

        finger_index = BaseBlock.skipchain_indices(new_index)
        newfingers += [f for f in current_block.fingers if f[0] in finger_index]

        new_block = cls(index=new_index, fingers=newfingers, _chain=_chain,
                        *args, **kwargs)
        return new_block

    def commit(self):
        """Commit the block onto the associated chain.
        """
        if self._chain is None:
            raise ValueError('Chain undefined.')
        self.pre_commit()
        self._chain.commit_block(self)

    def pre_commit(self):
        """Pre-commit hook.

        Expected to be overriden.
        """
        pass

    def __eq__(self, other):
        return self.hid == other.hid

    def __repr__(self):
        return ('{self.__class__.__name__}(index={self.index}, '
                'fingers={self.fingers}, _chain={self._chain}, '
                'payload={self.payload})').format(self=self)

    def serialize(self):
        raise NotImplementedError()

    @staticmethod
    def deserialize(serialized):
        raise NotImplementedError()


class Chain(object):
    """Verifiable skip chain backed by an object store.

    This class handles all interactions with the backend.

    .. warning::

       All read accesses are cached. The cache is assumed to be trusted,
       so blocks retrieved from cache are not checked for integrity, unlike
       when they are retrieved from the object store.

    """

    def __init__(self, object_store=None, head=None, block_cls=None,
                 cache=None):
        """
        :param object_store: Object store, e.g. :class:`DictStore` object
        :param head: The hash of the head block
        :param block_cls: Block class
        :param cache: Trusted cache
        :type cache: dict
        """
        super(Chain, self).__init__()

        self.object_store = object_store
        self.cache = cache or {}

        if hasattr(Chain, 'block_cls') and block_cls is None:
            self.block_cls = Chain.block_cls
        elif block_cls is not None:
            self.block_cls = block_cls
        else:
            raise ValueError('Block class must be provided either as param '
                             'to __init__ or as a class attribute.')

        self.head = head

    def __len__(self):
        try:
            return self.head_block.index + 1
        except AttributeError:
            return 0

    @property
    def head_block(self):
        return self.get_block_by_hid(self.head)

    def get_block_cls(self):
        try:
            return self.block_cls
        except AttributeError:
            raise NotImplemented

    def make_next_block(self, *args, **kwargs):
        return self.get_block_cls().make_next_block(
                current_block=self.head_block, _chain=self,
                *args, **kwargs)

    def commit_block(self, block):
        """Put the block into the object store."""
        # TODO: Validate fingers
        self.head = block.hid
        self.cache[block.hid] = block
        self.object_store.put(block.serialize())

    def get_block_by_hid(self, hid):
        """Retrieve block by its hash.

        :param hid: Block hash
        """
        if hid in self.cache:
            return self.cache[hid]

        block = raw_block = self.object_store.get(hid, check_integrity=True)
        if block is not None:
            block = self.get_block_cls().deserialize(raw_block)
            self.cache[hid] = block

        return block

    def get_block_by_index(self, index, return_evidence=False):
        """Retrieve a block by its index.

        Optionally returns a bundle of evidence.

        :param index: Block index
        :param return_evidence: Whether to return evidence
        :return: Found block or None, or (block, evidence) tuple if
                 return_evidence is True
        """
        if self.head is None:
            return None

        if return_evidence:
            evidence = OrderedDict()

        if not (0 <= index <= self.head_block.index):
            raise ValueError(
                    ("Block is beyond this chain head. Must be"
                     "0 <= {} <= {}.").format(index, self.head_block.index))

        hid = self.head
        current_block = self.head_block

        while current_block is not None:
            if return_evidence:
                evidence[hid] = current_block

            # When found:
            if index == current_block.index:
                if return_evidence:
                    return (current_block, evidence)
                return current_block

            # Otherwise, follow the fingers:
            _, hid = [(f, h) for (f, h) in current_block.fingers
                      if f >= index][0]
            current_block = self.get_block_by_hid(hid)

    def __repr__(self):
        return ('{self.__class__.__name__}(object_store={self.object_store}, '
                'head={self.head}, block_cls={self.block_cls})').format(
                        self=self)


