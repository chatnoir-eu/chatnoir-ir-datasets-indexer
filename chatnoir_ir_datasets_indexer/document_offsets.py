#!/usr/bin/env python3
from click import option, argument, command
import json
import gzip
from tqdm import tqdm
from typing import Dict
from pathlib import Path

def parse_documents(path: Path, doc_id='docno') -> Dict:
    """
    Parses a documents file and returns a dictionary with the document id as key and the byte range as value.
     
    This method is inspired by indxr, please cite: https://github.com/AmenRa/indxr

    :param path: Path to the document file in jsonl format as used in TIREx
    :return: Dictionary with the document id as key and the byte range as value
    """
    ret = {}

    with open(path, "rb") as file:
        position = file.tell()

        for _, line in tqdm(enumerate(file)):
            q = json.loads(line.decode())[doc_id]
            
            if q is None or q in ret:
                raise ValueError(f'Documents contains duplicate or null document ids. Got {q}')

            ret[q] = {'start': position, 'end': file.tell()}
            position = file.tell()

    return ret

@command("main")
@option("--docno", type=str, required=False, default='docno')
@argument("path", type=Path, required=True)
@argument("output-file", type=Path, required=True)
def main(path: Path, docno:str, output_file: Path):
    parsed_documents = parse_documents(path, docno)
    with gzip.open(output_file, 'wt') as f:
        f.write(json.dumps(parsed_documents))

if __name__ == '__main__':
    main()

