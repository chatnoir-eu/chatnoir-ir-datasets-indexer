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

### TREC TOT

`md5sum ~/.ir_datasets/trec-tot/2024/corpus.jsonl` gives: `0c535ac8d5cee481add41543bc8cb854`.

Upload data to s3:
```
s3cmd mb s3://corpus-trec-tot-2024
s3cmd put corpus.jsonl s3://corpus-trec-tot-2024/corpus.jsonl
```

Create document offsets:
```
./chatnoir_ir_datasets_indexer/document_offsets.py ~/.ir_datasets/trec-tot/2024/corpus.jsonl trec-tot-offsets.json.gz
```

