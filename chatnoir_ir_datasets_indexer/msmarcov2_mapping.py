from pathlib import Path
from datetime import datetime
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from chatnoir_ir_datasets_indexer.index import DatasetMapping, _DocumentType, _MetaRecordType, _DataRecordType, MetaRecord, DataRecord
from urllib.parse import urlparse
import gzip
import json
from tqdm import tqdm

class MsMarcoV2DocumentMapping(DatasetMapping):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    corpus_prefix = 'msmarco-v2-document'
    base_dir = Path('.')

    def __init__(self):
        with gzip.open('msmarco-v2-document-offsets.json.gz', 'rt') as f:
            self.warc_offsets = json.load(f)

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/21', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        offset = self.warc_offsets[doc.doc_id]
        return MetaRecord(
            source_file='s3://corpus-msmarco-document-v2/corpus.jsonl',
            source_offset=offset['start'],
            content_length=offset['start'] - offset['start'],
            content_type='application/json',
            uuid=self.webis_id(doc),
            warc_trec_id=doc.doc_id,
        )

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
        return Path('corpus.jsonl')

    def warc_offset(self, doc: _DocumentType) -> int:
        return self.warc_offsets[doc.doc_id]['start']

class MsMarcoV2PassageMapping(DatasetMapping):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    corpus_prefix = 'msmarco-v2-passage'
    base_dir = Path('.')

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/21', '%m/%y')

    def __init__(self):
        self.metadata_for_docs = {}
        with open('/mnt/ceph/tira/data/publicly-shared-datasets/ms-marco-document-v2/documents.jsonl', 'r') as f:
            for l in tqdm(f, 'load metadata'):
                l = json.loads(l)['original_document']
                self.metadata_for_docs[l['doc_id']] = {'title': l['title'], 'url': l['url']}

        with gzip.open('msmarco-v2-passage-offsets.json.gz', 'rt') as f:
            self.warc_offsets = json.load(f)

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        offset = self.warc_offsets[doc.doc_id]
        return MetaRecord(
            source_file='s3://corpus-msmarco-passage-v2/corpus.jsonl',
            source_offset=offset['start'],
            content_length=offset['start'] - offset['start'],
            content_type='application/json',
            uuid=self.webis_id(doc),
            warc_trec_id=doc.doc_id,
        )

    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        main_content = doc.default_text()
        metadata_for_doc = self.metadata_for_docs[doc.msmarco_document_id]
        
        parse_url = urlparse(metadata_for_doc['url'])
        return DataRecord(
            uuid=self.webis_id(doc),
            lang='en',
            warc_date=None,
            warc_record_id=None,
            warc_trec_id=doc.doc_id,
            warc_target_uri=metadata_for_doc['url'],
            warc_target_hostname=parse_url.hostname,
            warc_target_path=parse_url.path,
            warc_target_query_string=parse_url.query,
            warc_target_uri_hash=None,
            http_date=None,
            http_content_type="text/html",
            title=metadata_for_doc['title'],
            meta_keywords=None,
            meta_desc=None,
            body=main_content,
            body_length=len(main_content),
            full_body=main_content,
            headings=None,
        )

    def warc_path(self, doc: _DocumentType) -> Path:
        return Path('corpus.jsonl')

    def warc_offset(self, doc: _DocumentType) -> int:
        return self.warc_offsets[doc.doc_id]['start']
