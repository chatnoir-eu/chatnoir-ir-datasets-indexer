from base64 import b64encode
from hashlib import blake2b
from uuid import uuid5, NAMESPACE_URL, UUID


def webis_uuid(corpus_prefix: str, internal_id: str) -> str:
    """
    Calculate a Webis document UUID based on a corpus prefix and
    an internal (not necessarily universally unique) doc ID.

    :param corpus_prefix: corpus prefix (e.g., clueweb09, cc15, ...)
    :param internal_id: internal doc ID (e.g., clueweb09-en0044-22-32198)
    :return: Webis UUID as truncated Base64 string
    """
    uuid_input = ":".join((corpus_prefix, internal_id))
    return b64encode(uuid5(NAMESPACE_URL, uuid_input).bytes)[:-2].decode()


def webis_index_uuid(
        unix_time_ms: int,
        warc_pos: int,
        warc_name: str,
        doc_id: str,
) -> str:
    """
    Calculate an index-friendly time-based UUIDv1 for a document.

    :param unix_time_ms: 64-bit UNIX timestamp of the document in milliseconds
    :param warc_pos: character offset in the WARC file
    :param warc_name: WARC file name string
    :param doc_id: document Webis UUID string
    :return: index UUID as truncated Base64 string
    """
    mask_low = (1 << 32) - 1
    mask_mid = ((1 << 16) - 1) << 32
    time_low = unix_time_ms & mask_low
    time_mid = (unix_time_ms & mask_mid) >> 32

    warc_pos = warc_pos & ((1 << 32) - 1)
    time_hi_version = ((warc_pos >> 16) & 0x3FFF) | 0x1000

    clock_seq = warc_pos & 0xFFFF
    clock_seq_hi_variant = ((clock_seq >> 8) & 0x3F) | 0x80
    clock_seq_low = clock_seq & 0x00FF

    name_hash = blake2b(warc_name.encode(), digest_size=3).digest()
    id_hash = blake2b(doc_id.encode(), digest_size=3).digest()
    node = int.from_bytes(name_hash + id_hash, 'big')

    fields = (
        time_low,
        time_mid,
        time_hi_version,
        clock_seq_hi_variant,
        clock_seq_low,
        node,
    )
    return b64encode(UUID(fields=fields).bytes)[:-2].decode()
