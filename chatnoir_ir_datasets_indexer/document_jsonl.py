#!/usr/bin/env python3
import ir_datasets
from ir_datasets_owi import register_to_ir_datasets
from tira.ir_datasets_loader import IrDatasetsLoader


if __name__ == '__main__':
    print('fooo')
    register_to_ir_datasets()
    irds_loader = IrDatasetsLoader()
    irds_dataset = irds_loader.load_irds("wows/owi/2025")
    for doc in irds_loader.yield_docs(irds_dataset, True, True, None):
        print(doc)
        break
#
#            self.write_lines_to_file(
#                self.yield_docs(dataset, include_original, skip_duplicate_ids, allowlist_path_ids),
#                output_dataset_path / "documents.jsonl",
#            )
