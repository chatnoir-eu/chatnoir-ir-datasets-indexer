from os import environ
from pathlib import Path
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping
from urllib.parse import urlparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from ir_datasets import load
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats import ClueWeb22BDoc
from ir_datasets.formats.clueweb22 import ClueWeb22DocId, ClueWeb22Format
from itertools import islice
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from tqdm.auto import tqdm

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


class _SkipRecord(RuntimeError):
    pass


_Document = TypeVar("_Document", bound=NamedTuple)


def _dataset_base_dir(dataset_id: str) -> Path:
    if "IR_DATASETS_HOME" in environ:
        ir_datasets_home = Path(environ["IR_DATASETS_HOME"])
    else:
        ir_datasets_home = Path.home() / ".ir_datasets"
    if dataset_id.startswith("clueweb22"):
        return ir_datasets_home / "clueweb22" / "corpus"
    raise NotImplementedError(
        f"Dataset base dir for ir_dataset {dataset_id} "
        f"is not implemented yet."
    )


def _warc_file_info(
        doc: _Document,
        dataset_id: str,
        dataset_base_dir: Path,
) -> Tuple[Path, int]:
    """
    Extract file name, start offset and end offset
    from ir_datasets document record.

    :param doc: ir_datasets document record.
    :param dataset_id: ir_datasets dataset ID.
    """

    if dataset_id.startswith("clueweb22/b"):
        doc: ClueWeb22BDoc = doc
        # Parse document ID into components.
        doc_id = ClueWeb22DocId.from_string(doc.doc_id)

        # Determine WARC base path.
        format_type = ClueWeb22Format.HTML
        format_path = dataset_base_dir / format_type.value.id
        # Determine WARC file path.
        doc_path_name = f"{doc_id.path}{format_type.value.extension}"
        doc_path = format_path / doc_path_name
        # Determine WARC offset path.
        offsets_path_name = f"{doc_id.path}" \
                            f"{format_type.value.offset_extension}"
        offsets_path = format_path / offsets_path_name
        # Read WARC offsets.
        with offsets_path.open("rt", encoding="utf8") as offsets_file:
            offset_line = next(
                islice(offsets_file, doc_id.doc, doc_id.doc + 1)
            )
            # Determine document WARC record offsets.
            offset = int(offset_line)
        return doc_path, offset
    raise NotImplementedError(
        f"Metadata extraction for ir_dataset {dataset_id} "
        f"is not implemented yet."
    )


def _meta_record(
        webis_id: str,
        doc: _Document,
        dataset_id: str,
        file_name: str,
        start_offset: int,
) -> Mapping[str, str]:
    """
    Extract metadata from ir_datasets document record.

    :param webis_id: Webis document UUID
    :param doc: ir_datasets document record.
    :param dataset_id: ir_datasets dataset ID.
    :param file_name: WARC file name.
    :param start_offset: WARC file start offset.
    """

    if dataset_id.startswith("clueweb22/b"):
        from ir_datasets.formats.clueweb22 import ClueWeb22BDoc
        doc: ClueWeb22BDoc = doc
        return {
            "uuid": webis_id,
            "source_file": file_name,
            "source_offset": start_offset,
            "warc_type": "response",
            "warc_target_uri": doc.url,
            "warc_target_uri_hash": doc.url_hash,
            "warc_date": doc.date.isoformat(timespec="seconds"),
            "warc_record_id": doc.record_id,
            "warc_trec_id": doc.doc_id,
            "warc_payload_digest": doc.payload_digest,
            "content_type": "application/http;msgtype=response",
            "content_length": len(doc.html),
            "http_content_type": "text/html",
            "http_content_length": len(doc.html),
            "content_encoding": "utf8",
        }
    else:
        raise NotImplementedError(
            f"Metadata extraction for ir_dataset {dataset_id} "
            f"is not implemented yet."
        )


def _data_record(
        webis_id: str,
        doc: _Document,
        dataset_id: str,
) -> Mapping[str, str]:
    """
    Parse WARC record payload into an index document.

    :param webis_id: Webis document UUID
    :param doc: ir_datasets document
    :param dataset_id: ir_datasets dataset ID.
    :return: index document dict
    """
    if dataset_id.startswith("clueweb22/b"):
        from ir_datasets.formats.clueweb22 import ClueWeb22BDoc
        doc: ClueWeb22BDoc = doc

        html_tree = HTMLTree.parse_from_bytes(doc.html, "utf8")
        if not html_tree.body:
            raise _SkipRecord("No body")

        content_full = extract_plain_text(
            html_tree,
            alt_texts=True,
            preserve_formatting=False,
        )
        if not content_full:
            raise _SkipRecord(
                "Document empty after full content extraction"
            )

        replacement_count = content_full.count("\ufffd")
        if replacement_count / len(content_full) > 0.1:
            raise _SkipRecord(
                "Document contains more than 10% Unicode "
                "replacement characters."
            )
        if replacement_count > 0:
            content_full = content_full.replace("\ufffd", " ")
            content_full = collapse_whitespace(content_full)

        main_content = doc.text
        if len(main_content) < 200:
            raise _SkipRecord(
                f"Main content too short ({len(main_content)} codepoints)."
            )

        parse_url = urlparse(doc.url)

        return {
            "uuid": webis_id,
            "warc_record_id": doc.record_id,
            "warc_trec_id": doc.doc_id,
            "warc_target_uri": doc.url,
            "warc_target_hostname": parse_url.hostname,
            "warc_target_path": parse_url.path,
            "warc_target_query_string": parse_url.query,
            "warc_target_uri_hash": doc.url_hash,
            "date": doc.date.isoformat(),
            "lang": doc.language,
            "content_type": "text/html",
            "body_length": len(doc.html),
            f"title_lang_{doc.language}": extract_title(html_tree),
            f"meta_keywords_{doc.language}":
                extract_meta_keywords(html_tree)[:8192],
            f"meta_desc_lang_{doc.language}":
                extract_meta_description(html_tree)[:8192],
            f"body_lang_{doc.language}": doc.text,
            f"full_body_lang_{doc.language}": content_full,
            f"headings_lang_{doc.language}":
                extract_headings(html_tree, 3),
        }
    else:
        raise NotImplementedError(
            f"Metadata extraction for ir_dataset {dataset_id} "
            f"is not implemented yet."
        )



def _docs_iter(
        start: Optional[int],
        end: Optional[int],
        dataset_id: str,
) -> Tuple[Iterator[_Document], int, int]:
    dataset: Dataset = load(dataset_id)
    if not dataset.has_docs():
        raise ValueError(f"Dataset {dataset_id} has no documents.")
    docs_iter = dataset.docs_iter()
    if start is not None or end is not None:
        docs_iter = docs_iter[start:end]
    docs_iter: Iterator[_Document]

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
    initial = start

    # noinspection PyTypeChecker
    docs_iter = tqdm(
        docs_iter,
        initial=initial,
        total=total,
        desc=f"Iterate dataset {dataset_id}"
    )
    return docs_iter, initial, total


def _doc_id_prefix(dataset_id: str) -> str:
    if dataset_id.startswith("clueweb22"):
        return "clueweb22"
    raise NotImplementedError(
        f"Document ID prefix for ir_dataset {dataset_id} "
        f"is not implemented yet."
    )


def _iter_actions(
        es_index_meta: str,
        es_index_data: str,
        s3_bucket: Optional[str],
        dataset_id: str,
        docs_iter: Iterator[_Document],
) -> Iterator[Mapping[str, str]]:
    dataset_base_dir = _dataset_base_dir(dataset_id)
    doc_id_prefix = _doc_id_prefix(dataset_id)
    for doc in docs_iter:
        path, offset = _warc_file_info(
            doc,
            dataset_id,
            dataset_base_dir,
        )
        file_name: str = str(path.relative_to(dataset_base_dir))
        if s3_bucket is not None:
            file_name = f"s3://{s3_bucket}/{file_name}"

        webis_id = webis_uuid(doc_id_prefix, doc.doc_id)
        record_time = int(doc.date.timestamp() * 1000)
        webis_index_id = webis_index_uuid(
            record_time,
            offset,
            file_name,
            webis_id,
        )

        try:
            meta = _meta_record(webis_id, doc, dataset_id, file_name, offset            )
            data = _data_record(webis_id, doc, dataset_id)
            meta_action = index_action(webis_index_id, es_index_meta, meta)
            data_action = index_action(webis_index_id, es_index_data, data)
            yield meta_action
            yield data_action
        except _SkipRecord as e:
            print(f"Skipping meta record for {webis_id}: {e}")
            continue


def _exists_index(es: Elasticsearch, es_index: str) -> bool:
    response = es.indices.exists(index=es_index)
    return response.meta.status == 200


def _num_shards_replicas(dataset_id: str) -> Tuple[int, int]:
    if dataset_id.startswith("clueweb22"):
        return 1, 0
        # return 20, 2
    raise NotImplementedError(
        f"Number of shards and replicas for ir_dataset {dataset_id} "
        f"is not implemented yet."
    )


def _create_data_index(
        es: Elasticsearch,
        es_index: str,
        dataset_id: str,
) -> bool:
    num_shards, num_replicas = _num_shards_replicas(dataset_id)
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
        dataset_id: str,
) -> bool:
    num_shards, num_replicas = _num_shards_replicas(dataset_id)
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
    client = Elasticsearch(
        hosts=[es_host],
        http_auth=(es_username, es_password),
    )
    if not _exists_index(client, es_index_meta):
        _create_meta_index(client, es_index_meta, dataset_id)
    if not _exists_index(client, es_index_data):
        _create_data_index(client, es_index_data, dataset_id)

    docs_iter, initial, total = _docs_iter( start, end, dataset_id)
    total_actions = (total - initial) * 2

    actions = _iter_actions(
        es_index_meta,
        es_index_data,
        s3_bucket,
        dataset_id,
        docs_iter,
    )
    actions = (dict(action) for action in actions)

    results = streaming_bulk(
        client,
        actions,
        yield_ok=True,
        max_retries=10,
        initial_backoff=60,
        max_backoff=3600,
        timeout="5m",
        raise_on_error=False,
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
