from pathlib import Path
from datetime import datetime
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from chatnoir_ir_datasets_indexer.index import DatasetMapping, _DocumentType, _MetaRecordType, _DataRecordType, MetaRecord, DataRecord
from urllib.parse import urlparse
import gzip

class TrecTot2024DocumentMapping(DatasetMapping):
    num_data_shards = 3
    num_data_replicas = 1
    num_meta_shards = 1
    num_meta_replicas = 1
    corpus_prefix = 'trec-tot-2024-document'
    base_dir = Path('.')

    def __init__(self):
        self.inserted_document_ids = set()
        with gzip.open('trec-tot-offsets.json.gz', 'rt') as f:
            self.warc_offset = json.load(f)

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/24', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        offset = self.warc_offset[doc]
        return MetaRecord(
            source_file='s3://corpus-trec-tot-2024/corpus.jsonl',
            source_offset=offset['start'],
            content_length=offset['start'] - offset['start'],
            content_type='application/json',
            uuid=self.webis_id(doc),
            warc_trec_id=doc.doc_id,
        )

    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        if doc.doc_id in self.inserted_document_ids:
            return None
        self.inserted_document_ids.add(doc.doc_id)
        main_content = doc.default_text()
        return DataRecord(
            uuid=self.webis_id(doc),
            lang='en',
            warc_date=None,
            warc_record_id=None,
            warc_trec_id=doc.doc_id,
            warc_target_uri='https://www.wikidata.org/wiki/' + doc.wikidata_id,
            warc_target_hostname='wikipedia.org',
            warc_target_path=None,
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
        raise ValueError('custom...')

    def warc_offset(self, doc: _DocumentType) -> int:
        raise ValueError('custom...')

