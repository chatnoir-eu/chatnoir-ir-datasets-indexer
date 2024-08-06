from pathlib import Path
from datetime import datetime
from typing import Iterator, Optional, Tuple, TypeVar, NamedTuple, Mapping, \
    Generic, TypedDict, Union, Any, Sequence
from chatnoir_ir_datasets_indexer.index import DatasetMapping, _DocumentType, _MetaRecordType, _DataRecordType, PlaintextMetaRecord, DataRecord

class VaswaniDocumentMapping(DatasetMapping):
    num_data_shards = 1
    num_data_replicas = 3
    num_meta_shards = 1
    num_meta_replicas = 3
    corpus_prefix = 'vaswani'
    base_dir = Path('.')

    def record_time(self, doc: _DocumentType) -> datetime:
        return datetime.strptime('01/10', '%m/%y')

    def meta_record(self, doc: _DocumentType, s3_bucket: str) -> Optional[_MetaRecordType]:
        return PlaintextMetaRecord(plaintext_source_file='', plaintext_source_offset=0, plaintext_content_type='application/json')

    def data_record(self, doc: _DocumentType) -> Optional[_DataRecordType]:
        main_content = doc.default_text()
        return DataRecord(
            uuid=self.webis_id(doc),
            lang='en',
            warc_date=None,
            warc_record_id=None,
            warc_trec_id=doc.doc_id,
            warc_target_uri=None,
            warc_target_hostname=None,
            warc_target_path=None,
            warc_target_query_string=None,
            warc_target_uri_hash=None,
            http_date=None,
            http_content_type="text/html",
            title=None,
            meta_keywords=None,
            meta_desc=None,
            body=main_content,
            body_length=len(main_content),
            full_body=main_content,
            headings=None,
        )

    def warc_path(self, doc: _DocumentType) -> Path:
        return Path(f'documents.jsonl')

    def warc_offset(self, doc: _DocumentType) -> int:
        return 0

