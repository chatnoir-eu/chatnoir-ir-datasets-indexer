#!/usr/bin/env python3
import ir_datasets
from ir_datasets_owi import register_to_ir_datasets
from tira.ir_datasets_loader import IrDatasetsLoader
from tqdm import tqdm
import gzip
import json
from tirex_tracker import tracking, ExportFormat

NAME="ChatNoir (Indexer: Document Offsets)"
DESCRIPTION="""ChatNoir uses an object storage for random document access to show the indexed versions of documents.

For this, ChatNoir uploads the data to an object storage (S3 at the moment) and uses offsets in the uploaded data for random document acccess.
This step calculates the document offsets and stores them so that the indexing step that follows can add the information on how to access a document (i.e., S3 URL + offsets) into the index."""

if __name__ == '__main__':
    register_to_ir_datasets()
    irds_loader = IrDatasetsLoader()
    irds_dataset = irds_loader.load_irds("wows/owi/2025")
    byte_offset = 0
    doc_to_offset = {}
    with tracking(export_file_path=".metadata/wows-owi-2025/document-offsets.yml", export_format=ExportFormat.IR_METADATA, system_name=NAME, system_description=DESCRIPTION):
        with open("wows-owi-2025/documents.jsonl.gz", "wb") as f:
            for doc in tqdm(irds_loader.yield_docs(irds_dataset, True, True, None)):
                doc_id = json.loads(doc)["docno"]
                doc_compressed = gzip.compress((doc + '\n').encode("utf-8"))
                doc_to_offset[doc_id] = {'start': byte_offset, 'end': byte_offset + len(doc_compressed)}
                byte_offset += len(doc_compressed)
                f.write(doc_compressed)
        with gzip.open(".metadata/wows-owi-2025/wows-owi-2025-offsets.json.gz", "wt") as f:
            f.write(json.dumps(doc_to_offset))
