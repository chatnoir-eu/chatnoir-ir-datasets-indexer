from abc import abstractmethod, ABC
from datetime import datetime
from os import environ
from pathlib import Path
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, BulkIndexError
from ir_datasets import load
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats import GenericDoc
from itertools import islice
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from tqdm.auto import tqdm

from chatnoir_ir_datasets_indexer import LOGGER
from chatnoir_ir_datasets_indexer.elasticsearch import index_action
from chatnoir_ir_datasets_indexer.html import extract_title, \
    extract_meta_description, extract_meta_keywords, \
    extract_headings
from chatnoir_ir_datasets_indexer.index_data import SETTINGS_DATA, \
    MAPPINGS_DATA
from chatnoir_ir_datasets_indexer.index_meta import SETTINGS_META, \
    MAPPINGS_META
from chatnoir_ir_datasets_indexer.text import collapse_whitespace
from chatnoir_ir_datasets_indexer.webis import webis_uuid, webis_index_uuid

_IR_DATASETS_HOME: Path
if "IR_DATASETS_HOME" in environ:
    _IR_DATASETS_HOME = Path(environ["IR_DATASETS_HOME"])
else:
    _IR_DATASETS_HOME = Path.home() / ".ir_datasets"

_REPLACEMENT_CHAR = "\N{REPLACEMENT CHARACTER}"

_LANGUAGE_FIELDS = {
    "title",
    "meta_keywords",
    "meta_desc",
    "body",
    "full_body",
    "headings",
}


class MetaRecord(TypedDict):
    uuid: str
    """
    Webis internal UUID.
    """
    source_file: str
    """
    Source file URL in S3.
    """
    source_offset: int
    """
    Source offset in the (compressed) source file to access the record.
    """
    warc_type: str
    """
    WARC record type (``WARC-Type`` header), e.g., ``response`` 
    or ``resource``.
    """
    warc_target_uri: str
    """
    WARC record target URL (``WARC-Target-URI`` header), i.e., the original URL 
    of the crawled document.
    """
    warc_warcinfo_id: Optional[str]
    """
    WARC record ``WARC-Warcinfo-ID`` header.
    """
    warc_date: datetime
    """
    WARC record date (``WARC-Date`` header) when the WARC record was created.
    
    Not to be confused with the actual document's date (``http_date``).
    """
    warc_record_id: str
    """
    WARC record ID (``WARC-Record-ID`` header).
    """
    warc_trec_id: Optional[str]
    """
    WARC record TREC ID (``WARC-TREC-ID`` header), i.e., the official 
    TREC ID of the document stored within.
    """
    warc_identified_payload_type: Optional[str]
    """
    WARC record identified payload type (``WARC-Identified-Payload-Type`` 
    header).
    """
    warc_payload_digest: Optional[str]
    """
    WARC record payload digest (``WARC-Payload-Digest`` header).
    """
    warc_block_digest: Optional[str]
    """
    WARC record block digest (``WARC-Block-Digest`` header).
    """
    warc_ip_address: Optional[str]
    """
    WARC record IP address (``WARC-IP-Address`` header).
    """
    warc_ip_address: Optional[str]
    """
    WARC record IP address (``WARC-IP-Address`` header).
    """
    content_type: str
    """
    WARCs record payload content type (``Content-Type`` header), 
    e.g., ``application/http; msgtype=response`` for ``response`` records 
    or ``text/http`` for ``resource`` records.
    
    Not to be confused with the actual document's 
    content type (``http_content_type``).
    """
    content_length: int
    """
    WARC record payload content length (``Content-Length`` header) in bytes.
    
    Not to be confused with the actual document's 
    content length (``http_content_length``).
    """
    http_date: datetime
    """
    Embedded document HTTP date to be returned by the ChatNoir cache (``Date`` 
    header).
    This often corresponds to the date when the document was crawled.
    
    Not to be confused with the WARC record's date (``warc_date``).
    """
    http_content_length: int
    """
    Embedded document HTTP content length to be returned by the ChatNoir
    cache (HTTP ``Content-Length`` header).
    
    Not to be confused with the WARC record's 
    payload content length (``content_length``).
    """
    http_content_type: str
    """
    Embedded document HTTP content type, e.g., ``text/html``, to be returned 
    by the ChatNoir cache (HTTP ``Content-Type`` header).
    
    Not to be confused with the WARC record's 
    payload content type (``content_type``).
    """
    content_encoding: str
    """
    Embedded document HTTP content charset, e.g., ``utf8``, to be used 
    for parsing HTML by the ChatNoir cache.
    
    Not to be confused with the HTTP ``Content-Encoding`` header.
    """


class PlaintextMetaRecord(MetaRecord):
    plaintext_source_file: str
    """
    Body source file URL in S3.
    """
    plaintext_source_offset: int
    """
    Body source offset in the (compressed) source file to access the record.
    """
    plaintext_content_type: str
    """
    Body payload content type, e.g., ``application/x-ndjson``.
    """


class DataRecord(TypedDict):
    uuid: str
    """
    Webis internal UUID.
    """
    lang: str
    """
    Document language code as specified in ISO 639-1.
    Fall back to ISO 639‑2 or ISO 639‑3 if no two-letter code is available.
    """
    warc_date: datetime
    """
    WARC record date (``WARC-Date`` header) when the WARC record was created.
    
    Not to be confused with the actual document's date (``http_date``).
    """
    warc_record_id: str
    """
    WARC record ID (``WARC-Record-ID`` header).
    """
    warc_trec_id: Optional[str]
    """
    WARC record TREC ID (``WARC-TREC-ID`` header), i.e., the official 
    TREC ID of the document stored within.
    """
    warc_target_uri: str
    """
    WARC record target URL (``WARC-Target-URI`` header), i.e., the original URL 
    of the crawled document.
    """
    warc_target_hostname: str
    """
    WARC record target URL hostname (from ``warc_target_uri``).
    """
    warc_target_path: str
    """
    WARC record target URL path (from ``warc_target_uri``).
    """
    warc_target_query_string: str
    """
    WARC record target URL query_string (from ``warc_target_uri``).
    """
    http_date: datetime
    """
    Embedded document HTTP date to be returned by the ChatNoir cache (``Date`` 
    header).
    This often corresponds to the date when the document was crawled.
    
    Not to be confused with the WARC record's date (``warc_date``).
    """
    http_content_type: str
    """
    Embedded document HTTP content type, e.g., ``text/html``, to be returned 
    by the ChatNoir cache (HTTP ``Content-Type`` header).
    
    Not to be confused with the WARC record's 
    payload content type (``content_type``).
    """
    title: str
    """
    Document title.
    """
    meta_keywords: Sequence[str]
    """
    Document keywords from meta tags.
    """
    meta_desc: str
    """
    Document description from meta tags.
    """
    body: str
    """
    Document main content.
    """
    body_length: int
    """
    Length of the main content, i.e., ``len(body)``.
    """
    full_body: str
    """
    Document full content.
    """
    headings: Sequence[str]
    """
    List of document headings, e.g., ``h1``, ``h2``, ``h3``.
    """


_DocumentType = TypeVar("_DocumentType", bound=NamedTuple)
_MetaRecordType = TypeVar("_MetaRecordType", bound=MetaRecord)
_DataRecordType = TypeVar("_DataRecordType", bound=DataRecord)


class DatasetMapping(
    Generic[_DocumentType, _MetaRecordType, _DataRecordType], ABC
):
    @property
    @abstractmethod
    def base_dir(self) -> Path:
        pass

    @property
    @abstractmethod
    def corpus_prefix(self) -> str:
        pass

    @property
    @abstractmethod
    def num_data_shards(self) -> int:
        pass

    @property
    @abstractmethod
    def num_data_replicas(self) -> int:
        pass

    @property
    @abstractmethod
    def num_meta_shards(self) -> int:
        pass

    @property
    @abstractmethod
    def num_meta_replicas(self) -> int:
        pass

    def webis_id(self, doc: _DocumentType) -> str:
        return webis_uuid(self.corpus_prefix, doc.doc_id)

    def webis_index_id(self, doc: _DocumentType, s3_bucket: str) -> str:
        file = self.warc_file(doc, s3_bucket)
        offset = self.warc_offset(doc)
        time = self.record_time(doc)
        webis_id = self.webis_id(doc)
        return webis_index_uuid(
            int(time.timestamp() * 1000),
            offset,
            file,
            webis_id,
        )

    @abstractmethod
    def record_time(self, doc: _DocumentType) -> datetime:
        pass

    @abstractmethod
    def warc_path(self, doc: _DocumentType) -> Path:
        """Determine WARC file path."""
        pass

    def warc_file(self, doc: _DocumentType, s3_bucket: str) -> str:
        relative_path = self.warc_path(doc).relative_to(self.base_dir)
        return f"s3://{s3_bucket}/{relative_path}"

    @abstractmethod
    def warc_offset(self, doc: _DocumentType) -> int:
        pass

    @abstractmethod
    def meta_record(
            self,
            doc: _DocumentType,
            s3_bucket: str,
    ) -> Optional[_MetaRecordType]:
        pass

    @abstractmethod
    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        pass


def _dataset_mapping(dataset_id: str) -> DatasetMapping:
    if dataset_id.startswith("clueweb22"):
        from chatnoir_ir_datasets_indexer.clueweb22_mapping import ClueWeb22Mapping
        return ClueWeb22Mapping()
    if dataset_id.startswith("msmarco-document-v1"):
        from chatnoir_ir_datasets_indexer.msmarcov1_mapping import MsMarcoV1DocumentMapping
        return MsMarcoV1DocumentMapping()
    if dataset_id.startswith("msmarco-passage-v1"):
        from chatnoir_ir_datasets_indexer.msmarcov1_mapping import MsMarcoV1PassageMapping
        return MsMarcoV1PassageMapping()
    if dataset_id.startswith("msmarco-document-v2.1/segmented"):
        from chatnoir_ir_datasets_indexer.msmarcov21_mapping import MsMarcoV21SegmentedDocumentMapping
        return MsMarcoV21SegmentedDocumentMapping()
    if dataset_id.startswith("msmarco-document-v2.1"):
        from chatnoir_ir_datasets_indexer.msmarcov21_mapping import MsMarcoV21DocumentMapping
        return MsMarcoV21DocumentMapping()
    if dataset_id == "vaswani":
        from chatnoir_ir_datasets_indexer.vaswani_mapping import VaswaniDocumentMapping
        return VaswaniDocumentMapping()
    if dataset_id == 'trec-tot/2024':
        from chatnoir_ir_datasets_indexer.trec_tot_2024_mapping import TrecTot2024DocumentMapping
        return TrecTot2024DocumentMapping()
    raise NotImplementedError(
        f"Dataset mapping for ir_datasets {dataset_id} is not implemented yet."
    )


def _iter_docs(
        start: Optional[int],
        end: Optional[int],
        dataset_id: str,
) -> Tuple[Iterator[_DocumentType], int]:
    dataset: Dataset = load(dataset_id)
    if not dataset.has_docs():
        raise ValueError(f"Dataset {dataset_id} has no documents.")
    docs_iter = dataset.docs_iter()
    if start is not None or end is not None:
        docs_iter = docs_iter[start:end]
    docs_iter: Iterator[_DocumentType]

    start_text = f"start at {start}" if start is not None else None
    end_text = f"end at {end}" if end is not None else None
    limit_texts = [text for text in (start_text, end_text) if text is not None]
    offset_text = f" ({', '.join(limit_texts)})" if limit_texts else ""

    total = dataset.docs_count()
    if start is None:
        start = 0
    if end is None:
        end = total
    if start < 0:
        start = total + start
    if end < 0:
        end = total + end
    total = end - start

    # noinspection PyTypeChecker
    docs_iter = tqdm(
        docs_iter,
        total=total,
        desc=f"Iterate dataset {dataset_id}{offset_text}",
    )
    return docs_iter, total


def _convert_field(value: Any) -> Union[str, Sequence[str]]:
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence):
        return [_convert_field(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)


def _convert_record(
        record: Mapping[str, Optional[Any]]
) -> Mapping[str, Union[str, Sequence[str]]]:
    record = dict(record)
    if "lang" in record:
        language = record["lang"]
        for language_field in _LANGUAGE_FIELDS:
            field_name = f"{language_field}_lang_{language}"
            record[field_name] = record.pop(language_field)
    return {
        key: _convert_field(value)
        for key, value in record.items()
        if value is not None
    }


def _iter_actions(
        es_index_meta: str,
        es_index_data: str,
        s3_bucket: Optional[str],
        dataset_mapping: DatasetMapping[
            _DocumentType, _MetaRecordType, _DataRecordType
        ],
        docs: Iterator[_DocumentType],
) -> Iterator[Mapping[str, str]]:
    for doc in docs:
        meta = dataset_mapping.meta_record(doc, s3_bucket)
        if meta is None:
            continue
        meta = _convert_record(meta)

        data = dataset_mapping.data_record(doc)
        if data is None:
            continue
        data = _convert_record(data)

        webis_index_id = dataset_mapping.webis_index_id(doc, s3_bucket)

        meta_action = index_action(webis_index_id, es_index_meta, meta)
        data_action = index_action(webis_index_id, es_index_data, data)

        yield meta_action
        yield data_action


def _exists_index(es: Elasticsearch, es_index: str) -> bool:
    response = es.indices.exists(index=es_index)
    return response.meta.status == 200


def _create_data_index(
        es: Elasticsearch,
        es_index: str,
        num_shards: int,
        num_replicas: int,
) -> bool:
    response = es.indices.create(
        index=es_index,
        settings={
            **SETTINGS_DATA,
            "index": {
                "number_of_shards": num_shards,
                "number_of_replicas": num_replicas,
            },
        },
        mappings=MAPPINGS_DATA,
    )
    return response.meta.status == 200


def _create_meta_index(
        es: Elasticsearch,
        es_index: str,
        num_shards: int,
        num_replicas: int,
) -> bool:
    response = es.indices.create(
        index=es_index,
        settings={
            **SETTINGS_META,
            "index": {
                "number_of_shards": num_shards,
                "number_of_replicas": num_replicas,
            },
        },
        mappings=MAPPINGS_META,
    )
    return response.meta.status == 200


def index(
        es_host: str,
        es_username: str,
        es_password: str,
        es_index_meta: str,
        es_index_data: str,
        s3_bucket: Optional[str],
        start: Optional[int],
        end: Optional[int],
        dataset_id: str,
) -> None:
    dataset_mapping = _dataset_mapping(dataset_id)

    # Set up Elasticsearch connection.
    client = Elasticsearch(
        hosts=[es_host],
        http_auth=(es_username, es_password),
    )

    # Create indices if they don't exist yet.
    if not _exists_index(client, es_index_meta):
        _create_meta_index(
            client,
            es_index_meta,
            dataset_mapping.num_meta_shards,
            dataset_mapping.num_meta_replicas,
        )
    if not _exists_index(client, es_index_data):
        _create_data_index(
            client,
            es_index_data,
            dataset_mapping.num_data_shards,
            dataset_mapping.num_data_replicas,
        )

    # Iterate over documents.
    docs, total = _iter_docs(start, end, dataset_id)
    total_actions = total * 2

    actions = _iter_actions(
        es_index_meta,
        es_index_data,
        s3_bucket,
        dataset_mapping,
        docs,
    )
    actions = (dict(action) for action in actions)

    try:
        results = streaming_bulk(
            client,
            actions,
            chunk_size=100,
            yield_ok=True,
            max_retries=10,
            initial_backoff=60,
            max_backoff=3600,
            timeout="5m",
            request_timeout=300,
        )
        results = tqdm(
            results,
            desc=f"Index dataset {dataset_id}",
            unit="action",
            total=total_actions,
        )
        for ok, item in results:
            if not ok:
                raise Exception(f"Failed to index with error: {item}")
    except BulkIndexError as e:
        raise Exception(f"Failed to index with errors: {e.errors}")
