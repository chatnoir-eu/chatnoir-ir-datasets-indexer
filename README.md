# chatnoir-ir-datasets-indexer

Simple indexer to integrate selected datasets from [ir_datasets](https://ir-datasets.com) into the [ChatNoir](https://chatnoir.eu) search engine.

## Installation

1. Install [Python 3.10](https://python.org/downloads/)
2. Install [pipx](https://pipxproject.github.io/pipx/installation/#install-pipx)
3. Install [Pipenv](https://pipenv.pypa.io/en/latest/install/#isolated-installation-of-pipenv-with-pipx).
4. Install dependencies:
    ```shell
    pipenv install
    ```

## Usage without Pipenv

I had problems with running it with PipEnv, hence, I used the one above.

```shell
./main.py
```

## Usage with Pipenv

```shell
pipenv run python -m chatnoir_ir_datasets_indexer
```

## Datasets in progress

### LongEval-SCI

```
./manage.py ir_datasets_loader_cli --ir_datasets_id 'longeval-sci/2024-11/train' --output_dataset_path inputs --output_dataset_truth_path truths
```

Upload data to s3:
```
s3cmd mb s3://corpus-longeval-sci-2024-11
s3cmd put corpus.jsonl s3://corpus-longeval-sci-2024-11/corpus.jsonl
```

Document offsets:
```
./chatnoir_ir_datasets_indexer/document_offsets.py --docno docno /tmp/corpus.jsonl longeval-sci-offsets.json.gz
```

```
python3 main.py \
	--data-index chatnoir_data_longeval_sci_2024_11 \
	--meta-index chatnoir_meta_longeval_sci_2024_11 \
	--username USER \
	--password PASSWORD \
	longeval-sci/2024-11/train
```

### TREC TOT

`md5sum ~/.ir_datasets/trec-tot/2024/corpus.jsonl` gives: `0c535ac8d5cee481add41543bc8cb854`.

Upload data to s3:
```
s3cmd mb s3://corpus-trec-tot-2024
s3cmd put corpus.jsonl s3://corpus-trec-tot-2024/corpus.jsonl
```

export IR-dataset

create documents.jsonl file (within tira repo, store in /mnt/ceph/tira for easy re-use):

```
./src/manage.py ...
```

Upload to S3:

```
s3cmd mb s3://corpus-msmarco-passage-v1
s3cmd put /mnt/ceph/tira/data/publicly-shared-datasets/msmarco-passage-trec-dl-v1/documents.jsonl s3://corpus-msmarco-passage-v1/corpus.jsonl




Create document offsets:
```
./chatnoir_ir_datasets_indexer/document_offsets.py --docno doc_id ~/.ir_datasets/trec-tot/2024/corpus.jsonl trec-tot-offsets.json.gz

./chatnoir_ir_datasets_indexer/document_offsets.py --docno docno /mnt/ceph/tira/data/publicly-shared-datasets/msmarco-passage-trec-dl-v1/documents.jsonl msmarco-v1-passage-offsets.json.gz



./chatnoir_ir_datasets_indexer/document_offsets.py --docno docno /mnt/ceph/tira/data/publicly-shared-datasets/ms-marco-document-v1/documents.jsonl msmarco-v1-document-offsets.json.gz

./chatnoir_ir_datasets_indexer/document_offsets.py --docno docno /mnt/ceph/tira/data/publicly-shared-datasets/ms-marco-document-v2/documents.jsonl msmarco-v2-document-offsets.json.gz

./chatnoir_ir_datasets_indexer/document_offsets.py --docno docno /mnt/ceph/tira/data/publicly-shared-datasets/ms-marco-passage-v2/documents.jsonl msmarco-v2-passage-offsets.json.gz



```

Index:
```
python3 main.py \
	--data-index chatnoir_data_trec_tot_2024 \
	--meta-index chatnoir_meta_trec_tot_2024 \
	--username USER \
	--password PASSWORD \
	trec-tot/2024
```
