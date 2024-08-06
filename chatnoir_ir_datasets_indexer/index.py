from abc import abstractmethod, ABC
from datetime import datetime
from os import environ
from pathlib import Path
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from urllib.parse import urlparse

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



class _ClueWeb22DataRecord(DataRecord):
    warc_target_uri_hash: str


class _ClueWeb22MetaRecord(PlaintextMetaRecord):
    warc_target_uri_hash: str


class ClueWeb22Mapping(
    DatasetMapping[GenericDoc, _ClueWeb22MetaRecord, _ClueWeb22DataRecord]
):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    base_dir = _IR_DATASETS_HOME / "clueweb22" / "corpus"
    corpus_prefix = "clueweb22"

    def record_time(self, doc: GenericDoc) -> datetime:
        return doc.date

    @staticmethod
    def _doc_id(doc: GenericDoc):
        """Parse document ID into components."""
        return ClueWeb22DocId.from_string(doc.doc_id)

    def _path(self, doc: GenericDoc, format_type) -> Path:
        name = f"{self._doc_id(doc).path}{format_type.extension}"
        return self.base_dir / format_type.id / name

    def _offset(self, doc: GenericDoc, format_type) -> int:
        doc_id = self._doc_id(doc)
        # Determine offset path.
        offsets_name = f"{doc_id.path}{format_type.offset_extension}"
        offsets_path = self.base_dir / format_type.id / offsets_name
        # Read offsets.
        with offsets_path.open("rt", encoding="utf8") as offsets_lines:
            # Seek to the document offset.
            offsets_lines = islice(offsets_lines, doc_id.doc, doc_id.doc + 1)
            # Determine current document offset.
            return int(next(offsets_lines))

    def warc_path(self, doc) -> Path:
        return self._path(doc, ClueWeb22Format.HTML)

    def warc_offset(self, doc) -> int:
        return self._offset(doc, ClueWeb22Format.HTML)

    def plaintext_path(self, doc) -> Path:
        return self._path(doc, ClueWeb22Format.TXT)

    def plaintext_file(self, doc: _DocumentType, s3_bucket: str) -> str:
        relative_path = self.plaintext_path(doc).relative_to(self.base_dir)
        return f"s3://{s3_bucket}/{relative_path}"

    def plaintext_offset(self, doc) -> int:
        return self._offset(doc, ClueWeb22Format.TXT)

    def meta_record(
            self,
            doc,
            s3_bucket: str,
    ) -> Optional[_ClueWeb22MetaRecord]:
        return _ClueWeb22MetaRecord(
            uuid=self.webis_id(doc),
            source_file=self.warc_file(doc, s3_bucket),
            source_offset=self.warc_offset(doc),
            warc_type="resource",
            warc_target_uri=doc.url,
            warc_target_uri_hash=doc.url_hash,
            warc_warcinfo_id=None,
            warc_date=doc.date,
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_identified_payload_type="text/html",
            warc_payload_digest=doc.payload_digest,
            warc_block_digest=None,
            warc_ip_address=None,
            content_type="text/html",
            content_length=len(doc.html),
            http_date=doc.date,
            http_content_type="text/html",
            http_content_length=len(doc.html),
            content_encoding="utf8",
            plaintext_source_file=self.plaintext_file(doc, s3_bucket),
            plaintext_source_offset=self.plaintext_offset(doc),
            plaintext_content_type="application/x-ndjson+clueweb22",
        )

    def data_record(
            self,
            doc,
    ) -> Optional[_ClueWeb22DataRecord]:
        html_tree = HTMLTree.parse_from_bytes(doc.html, "utf8")
        if not html_tree.body:
            LOGGER.info(f"Skipping document {doc.doc_id}: No body.")
            return None

        content_full = extract_plain_text(
            html_tree,
            alt_texts=True,
            preserve_formatting=False,
        )
        if len(content_full) <= 0:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Document empty after full content extraction."
            )
            return None

        replacement_count = content_full.count(_REPLACEMENT_CHAR)
        if replacement_count / len(content_full) > 0.1:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Document contains more than 10% Unicode "
                f"replacement characters."
            )
            return None
        if replacement_count > 0:
            content_full = content_full.replace(_REPLACEMENT_CHAR, " ")
            content_full = collapse_whitespace(content_full)

        main_content = doc.text
        if len(main_content) < 200:
            LOGGER.info(
                f"Skipping document {doc.doc_id}: "
                f"Main content too short ({len(main_content)} codepoints)."
            )
            return None

        parse_url = urlparse(doc.url)
        title = extract_title(html_tree)
        meta_keywords = extract_meta_keywords(html_tree)
        meta_description = extract_meta_description(html_tree)

        language = doc.language
        if "_" in language:
            language = language.split("_")[0]

        return _ClueWeb22DataRecord(
            uuid=self.webis_id(doc),
            lang=language,
            warc_date=doc.date,
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_target_uri=doc.url,
            warc_target_hostname=parse_url.hostname,
            warc_target_path=parse_url.path,
            warc_target_query_string=parse_url.query,
            warc_target_uri_hash=doc.url_hash,
            http_date=doc.date,
            http_content_type="text/html",
            title=title,
            meta_keywords=meta_keywords,
            meta_desc=meta_description,
            body=main_content,
            body_length=len(main_content),
            full_body=content_full,
            headings=extract_headings(html_tree, 3),
        )

class MsMarcoV21SegmentedDocumentMapping(DatasetMapping):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    corpus_prefix = 'msmarco-v2.1-document-segmented'
    base_dir = Path('.')

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/22', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        return PlaintextMetaRecord(plaintext_source_file='', plaintext_source_offset=0, plaintext_content_type='application/json')

    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        parse_url = urlparse(doc.url)
        main_content = doc.default_text()
        return DataRecord(
            uuid=self.webis_id(doc),
            lang='en',
            warc_date=None,
            warc_record_id=None,
            warc_trec_id=doc.doc_id,
            warc_target_uri=doc.url,
            warc_target_hostname=parse_url.hostname,
            warc_target_path=parse_url.path,
            warc_target_query_string=parse_url.query,
            warc_target_uri_hash=None,
            http_date=None,
            http_content_type="text/html",
            title=doc.title,
            meta_keywords=None,
            meta_desc=None,
            body=main_content,
            body_length=len(main_content),
            full_body=main_content,
            headings=None,
        )

    def warc_path(self, doc: _DocumentType) -> Path:
        (string1, string2, string3, bundlenum, doc_position, position) = doc.doc_id.split("_")
        assert string1 == "msmarco" and string2 == "v2.1" and string3 == "doc"
        return Path(f'msmarco_v2.1_doc_segmented_{bundlenum}.json')

    def warc_offset(self, doc: _DocumentType) -> int:
        return int(doc.doc_id.split('_')[-1])


def _dataset_mapping(dataset_id: str) -> DatasetMapping:
    if dataset_id.startswith("clueweb22/a"):
        return ClueWeb22Mapping()
    if dataset_id.startswith("clueweb22/b"):
        return ClueWeb22Mapping()
    if dataset_id.startswith("msmarco-document-v2.1/segmented"):
        return MsMarcoV21SegmentedDocumentMapping()
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
