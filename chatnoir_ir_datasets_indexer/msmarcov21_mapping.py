from pathlib import Path
from datetime import datetime
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from chatnoir_ir_datasets_indexer.index import DatasetMapping, _DocumentType, _MetaRecordType, _DataRecordType, MetaRecord, DataRecord
from urllib.parse import urlparse

class MsMarcoV21DocumentMapping(DatasetMapping):
    num_data_shards = 40
    num_data_replicas = 1
    num_meta_shards = 10
    num_meta_replicas = 1
    corpus_prefix = 'msmarco-v2.1-document'
    base_dir = Path('.')

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/22', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        return MetaRecord(
            source_file='s3://corpus-msmarco-document-v2.1/' + str(self.warc_path(doc)),
            source_offset=self.warc_offset(doc),
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
        (string1, string2, string3, bundlenum, position) = doc.doc_id.split("_")
        assert string1 == "msmarco" and string2 == "v2.1" and string3 == "doc"
        return Path(f'msmarco_v2.1_doc_{bundlenum}.json')

    def warc_offset(self, doc: _DocumentType) -> int:
        return int(doc.doc_id.split('_')[-1])

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
        return MetaRecord(
            source_file='s3://corpus-msmarco-document-v2.1-segmented/' + str(self.warc_path(doc)),
            source_offset=self.warc_offset(doc),
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
        (string1, string2, string3, bundlenum, doc_position, position) = doc.doc_id.split("_")
        assert string1 == "msmarco" and string2 == "v2.1" and string3 == "doc"
        return Path(f'msmarco_v2.1_doc_segmented_{bundlenum}.json')

    def warc_offset(self, doc: _DocumentType) -> int:
        return int(doc.doc_id.split('_')[-1])
