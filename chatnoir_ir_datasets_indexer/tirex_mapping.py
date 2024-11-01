from pathlib import Path
from datetime import datetime
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from chatnoir_ir_datasets_indexer.index import DatasetMapping, _DocumentType, _MetaRecordType, _DataRecordType, MetaRecord, DataRecord
from urllib.parse import urlparse
import gzip
import json

class TirexMapping(DatasetMapping):
    num_data_shards = 10
    num_data_replicas = 3
    num_meta_shards = 1
    num_meta_replicas = 3
    base_dir = Path('.')

    def __init__(self, dataset_id):
        self.__corpus_prefix = dataset_id.replace('/', '-').lower()
        with gzip.open(self.__corpus_prefix + '-offsets.json.gz', 'rt') as f:
            self.warc_offsets = json.load(f)

    @property
    def corpus_prefix(self):
        return self.__corpus_prefix

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/18', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        offset = self.warc_offsets[doc.doc_id]
        return MetaRecord(
            source_file=f's3://corpora-tirex-small/{self.corpus_prefix}-corpus.jsonl',
            source_offset=offset['start'],
            content_length=offset['start'] - offset['start'],
            content_type='application/json',
            uuid=self.webis_id(doc),
            warc_trec_id=doc.doc_id,
        )

    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        parse_url = urlparse(doc.url) if hasattr(doc, 'url') else None

        main_content = doc.default_text()
        return DataRecord(
            uuid=self.webis_id(doc),
            lang='en',
            warc_date=None,
            warc_record_id=None,
            warc_trec_id=doc.doc_id,
            warc_target_uri=doc.url if hasattr(doc, 'url') else None,
            warc_target_hostname=parse_url.hostname if parse_url else None,
            warc_target_path=parse_url.path if parse_url else None,
            warc_target_query_string=parse_url.query  if parse_url else None,
            warc_target_uri_hash=None,
            http_date=None,
            http_content_type="text/html",
            title=doc.title if hasattr(doc, 'title') else f'Document {doc.doc_id}',
            meta_keywords=None,
            meta_desc=None,
            body=main_content,
            body_length=len(main_content),
            full_body=main_content,
            headings=None,
        )

    def warc_path(self, doc: _DocumentType) -> Path:
        return Path(f'{self.corpus_prefix}-corpus.jsonl')

    def warc_offset(self, doc: _DocumentType) -> int:
        return self.warc_offsets[doc.doc_id]['start']
