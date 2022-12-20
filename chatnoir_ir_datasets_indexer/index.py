from abc import abstractmethod, ABC
from datetime import datetime
from os import environ
from pathlib import Path
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from ir_datasets import load
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats import ClueWeb22BDoc
from ir_datasets.formats.clueweb22 import ClueWeb22DocId, ClueWeb22Format, \
    ClueWeb22ADoc
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
    source_file: str
    """
    Source file URL in S3.
    """
    source_offset: int
    """
    Source offset in the (compressed) source file to access the record.
    """
    warc_type: str
    warc_target_uri: str
    warc_warcinfo_id: Optional[str]
    warc_date: datetime
    warc_record_id: str
    warc_trec_id: str
    warc_identified_payload_type: str
    warc_payload_digest: str
    warc_block_digest: Optional[str]
    content_type: str
    """
    Record content type, e.g. ``application/http; msgtype=response``.
    """
    content_length: int
    """
    Record content length, in bytes.
    """
    http_content_length: int
    """
    Payload length, in bytes.
    """
    http_content_type: str
    """
    Payload content type, e.g., ``text/html``.
    """
    content_encoding: str
    """
    Payload encoding, e.g., ``utf8``.
    """


class BodyMetaRecord(MetaRecord):
    body_source_file: str
    """
    Body source file URL in S3.
    """
    body_source_offset: int
    """
    Body source offset in the (compressed) source file to access the record.
    """
    body_content_type: str
    """
    Body payload content type, e.g., ``application/x-ndjson``.
    """


class DataRecord(TypedDict):
    uuid: str
    warc_record_id: str
    warc_trec_id: str
    warc_target_uri: str
    warc_target_hostname: str
    warc_target_path: str
    warc_target_query_string: str
    date: datetime
    lang: str
    content_type: str
    body_length: int
    title: str
    meta_keywords: Sequence[str]
    meta_desc: str
    body: str
    full_body: str
    headings: Sequence[str]


_DocumentType = TypeVar("_DocumentType", bound=NamedTuple)
_MetaRecordType = TypeVar("_MetaRecordType", bound=MetaRecord)
_DataRecordType = TypeVar("_DataRecordType", bound=DataRecord, )


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
    def num_shards(self) -> int:
        pass

    @property
    @abstractmethod
    def num_replicas(self) -> int:
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


_ClueWeb22Doc = Union[ClueWeb22ADoc, ClueWeb22BDoc]


class _ClueWeb22DataRecord(DataRecord):
    warc_target_uri_hash: str


class ClueWeb22Mapping(
    DatasetMapping[_ClueWeb22Doc, BodyMetaRecord, _ClueWeb22DataRecord]
):
    num_shards = 20
    num_replicas = 0
    base_dir = _IR_DATASETS_HOME / "clueweb22" / "corpus"
    corpus_prefix = "clueweb22"

    def record_time(self, doc: _ClueWeb22Doc) -> datetime:
        return doc.date

    @staticmethod
    def _doc_id(doc: _ClueWeb22Doc) -> ClueWeb22DocId:
        """Parse document ID into components."""
        return ClueWeb22DocId.from_string(doc.doc_id)

    def _path(self, doc: _ClueWeb22Doc, format_type: ClueWeb22Format) -> Path:
        name = f"{self._doc_id(doc).path}{format_type.extension}"
        return self.base_dir / format_type.id / name

    def _offset(self, doc: _ClueWeb22Doc, format_type: ClueWeb22Format) -> int:
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

    def warc_path(self, doc: _ClueWeb22Doc) -> Path:
        return self._path(doc, ClueWeb22Format.HTML)

    def warc_offset(self, doc: _ClueWeb22Doc) -> int:
        return self._offset(doc, ClueWeb22Format.HTML)

    def body_path(self, doc: _ClueWeb22Doc) -> Path:
        return self._path(doc, ClueWeb22Format.TXT)

    def body_file(self, doc: _DocumentType, s3_bucket: str) -> str:
        relative_path = self.body_path(doc).relative_to(self.base_dir)
        return f"s3://{s3_bucket}/{relative_path}"

    def body_offset(self, doc: _ClueWeb22Doc) -> int:
        return self._offset(doc, ClueWeb22Format.TXT)

    def meta_record(
            self,
            doc: _ClueWeb22Doc,
            s3_bucket: str,
    ) -> Optional[BodyMetaRecord]:
        return BodyMetaRecord(
            uuid=self.webis_id(doc),
            source_file=self.warc_file(doc, s3_bucket),
            source_offset=self.warc_offset(doc),
            warc_type="resource",
            warc_target_uri=doc.url,
            # warc_target_uri_hash=doc.url_hash,
            warc_warcinfo_id=None,
            warc_date=doc.date,
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_identified_payload_type="text/html",
            warc_payload_digest=doc.payload_digest,
            warc_block_digest=None,
            content_type="text/html",
            content_length=len(doc.html),
            http_content_type="text/html",
            http_content_length=len(doc.html),
            content_encoding="utf8",
            body_source_file=self.body_file(doc, s3_bucket),
            body_source_offset=self.body_offset(doc),
            body_content_type="application/x-ndjson+clueweb22",
        )

    def data_record(
            self,
            doc: _ClueWeb22Doc,
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

        return _ClueWeb22DataRecord(
            uuid=self.webis_id(doc),
            warc_record_id=str(doc.record_id),
            warc_trec_id=doc.doc_id,
            warc_target_uri=doc.url,
            warc_target_hostname=parse_url.hostname,
            warc_target_path=parse_url.path,
            warc_target_query_string=parse_url.query,
            warc_target_uri_hash=doc.url_hash,
            date=doc.date,
            lang=doc.language,
            content_type="text/html",
            body_length=len(doc.html),
            title=title,
            meta_keywords=meta_keywords,
            meta_desc=meta_description,
            body=main_content,
            full_body=content_full,
            headings=extract_headings(html_tree, 3),
        )


def _dataset_mapping(dataset_id: str) -> DatasetMapping:
    if dataset_id.startswith("clueweb22/a"):
        return ClueWeb22Mapping()
    if dataset_id.startswith("clueweb22/b"):
        return ClueWeb22Mapping()
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
    num_shards = dataset_mapping.num_shards
    num_replicas = dataset_mapping.num_replicas
    if not _exists_index(client, es_index_meta):
        _create_meta_index(client, es_index_meta, num_shards, num_replicas)
    if not _exists_index(client, es_index_data):
        _create_data_index(client, es_index_data, num_shards, num_replicas)

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
