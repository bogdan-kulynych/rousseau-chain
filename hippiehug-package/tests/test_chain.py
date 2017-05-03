import six
import mock
import json
import pytest

# TODO: Change to concrete names
from hippiehug.Chain import *
from hippiehug.Store import DictionaryStore
from hippiehug.Utils import IntegrityValidationError


class SerializableBlock(BaseBlock):
    def serialize(self):
        return json.dumps({
            'index': self.index,
            'payload': self.payload,
            'fingers': self.fingers
        }).encode('utf-8')

    @staticmethod
    def deserialize(serialized):
        kwargs = json.loads(serialized.decode('utf-8'))
        return BaseBlock(**kwargs)


@pytest.fixture(params=[dict])
def object_store(request):
    backend = request.param()
    return DictionaryStore(backend)


@pytest.fixture(params=['Test.'])
def block(request):
    return SerializableBlock(payload=request.param)


@pytest.fixture
def chain_factory(object_store):
    class ChainFactory:
        def make(self, block_cls=BaseBlock):
            return Chain(object_store, block_cls=block_cls)
    yield ChainFactory()


CHAIN_SIZE = 3


@pytest.fixture(params=[CHAIN_SIZE])
def chain_and_hids(request, chain_factory):
    chain = chain_factory.make(block_cls=SerializableBlock)
    hids = []
    for i in range(request.param):
        block = chain.make_next_block()
        block.payload = 'Block {}'.format(i)
        block.commit()
        hids.append(block.hid)
    return chain, hids


def test_serialization(block):
    """
    Test if the utility class serializes fine.
    """
    a = block
    serialized = a.serialize()
    assert json.loads(serialized.decode('utf-8')) == {
        'index': 0,
        'payload': a.payload,
        'fingers': []
    }

    b = SerializableBlock.deserialize(serialized)
    assert a.index == b.index \
           and a.fingers == b.fingers \
           and a.payload == b.payload


def test_block_hid(block):
    """
    Check if blocks can be hashed.
    """
    assert block.hid == ascii_hash(block.serialize())


def test_block_repr(block):
    assert 'Block(' in repr(block)


def test_chain_repr(chain_factory):
    chain = chain_factory.make()
    assert 'Chain(' in repr(chain)


def test_make_next_block_doesnt_change_chain(chain_factory):
    """
    chain.make_next_block() should not modify the chain before
    the block is commited.
    """
    chain = chain_factory.make()
    assert len(chain) == 0
    block = chain.make_next_block()
    assert len(chain) == 0


def test_commit_blocks(chain_factory):
    """
    Commit couple of blocks and check that the chain head
    moves.
    """
    chain = chain_factory.make(block_cls=SerializableBlock)

    expected_head = None

    for i in range(5):
        assert len(chain) == i
        assert chain.head == expected_head

        block = chain.make_next_block()
        block.payload = 'Block {}'.format(i)
        block.commit()
        expected_head = block.hid


def test_commit_fails_if_chain_undefined():
    """
    Commiting the block should fail if no chain is associated.
    """
    block = SerializableBlock()
    with pytest.raises(ValueError):
        block.commit()


def test_get_block_by_hid_from_cache(chain_and_hids):
    """
    Check that blocks are retrievable by hashes from cache.
    """
    chain, hids = chain_and_hids
    for i, hid in enumerate(hids):
        block = chain.get_block_by_hid(hid)
        assert block.payload == 'Block {}'.format(i)
    return block


def test_get_block_by_hid_from_store(chain_and_hids):
    """
    Check that blocks are retrievable by hashes with cache cleared.
    """
    chain, hids = chain_and_hids
    for i, hid in enumerate(hids):
        chain.cache.clear()
        block = chain.get_block_by_hid(hid)
        assert block.payload == 'Block {}'.format(i)
    return block


def test_get_block_by_hid_fails_if_hash_wrong(chain_and_hids):
    """
    Check that exception is thrown if the hash is incorrect.
    """
    chain, hids = chain_and_hids
    chain.cache.clear()
    target_hid = hids[0]

    substitute_block = SerializableBlock(payload='Hacked')
    chain.object_store._backend[target_hid] = substitute_block.serialize()

    with pytest.raises(IntegrityValidationError):
        chain.get_block_by_hid(target_hid)


def test_get_block_by_index_from_cache(chain_and_hids):
    """
    Check that blocks are retrievable by index from cache
    """
    chain, hids = chain_and_hids
    for i, _ in enumerate(hids):
        block = chain.get_block_by_index(i)
        assert block.payload == 'Block {}'.format(i)
    return block


def test_get_block_by_index_from_store(chain_and_hids):
    """
    Check that blocks are retrievable by index from cache
    """
    chain, hids = chain_and_hids
    for i, _ in enumerate(hids):
        chain.cache.clear()
        block = chain.get_block_by_index(i)
        assert block.payload == 'Block {}'.format(i)
    return block


@pytest.mark.parametrize('index', [-1, CHAIN_SIZE + 1])
def test_get_block_by_index_fails_if_block_out_of_range(chain_and_hids, index):
    """
    Check that exception is thrown if the hash is incorrect.
    """
    chain, hids = chain_and_hids
    with pytest.raises(ValueError):
        chain.get_block_by_index(index)


def test_chain_evidence(chain_factory, object_store):
    """
    Check returned evidence
    """
    chain1 = chain_factory.make(block_cls=SerializableBlock)
    for i in range(10):
        chain1.make_next_block(payload='Block %i' % i).commit()

    res, evidence = chain1.get_block_by_index(2, return_evidence=True)
    serialized_evidence = {hid: block.serialize()
                           for hid, block in evidence.items()}

    chain2 = chain_factory.make(block_cls=SerializableBlock)
    chain2.head = chain1.head
    chain2.object_store = DictionaryStore(serialized_evidence)
    assert chain2.get_block_by_index(5).payload == 'Block 5'

