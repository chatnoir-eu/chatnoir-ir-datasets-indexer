#!/usr/bin/env python3
from logging import INFO, basicConfig
from typing import Optional

from click import option, argument, STRING, INT, command, BOOL

from chatnoir_ir_datasets_indexer import LOGGER
from chatnoir_ir_datasets_indexer.index import index
from tirex_tracker import tracking, ExportFormat
from pathlib import Path

_DEFAULT_ES_HOST = "https://elasticsearch.srv.webis.de:9200"


@command("main")
@option(
    "--host", "--es-host", "-e",
    type=STRING,
    required=True,
    envvar="ES_HOST",
    default=_DEFAULT_ES_HOST,
)
@option(
    "--username", "--es-username", "-u",
    type=STRING,
    required=True,
    envvar="ES_USERNAME",
    prompt="Elasticsearch username",
)
@option(
    "--password", "--es-password", "-p",
    type=STRING,
    required=True,
    envvar="ES_PASSWORD",
    prompt="Elasticsearch password",
    hide_input=True,
)
@option(
    "--meta-index", "--es-meta-index", "-m",
    type=STRING,
    required=True,
)
@option(
    "--data-index", "--es-data-index", "-d",
    type=STRING,
    required=True,
)
@option(
    "--bucket", "--s3-bucket", "-b",
    type=STRING,
    required=False,
)
@option(
    "--start",
    type=INT,
    required=False,
)
@option(
    "--end",
    type=INT,
    required=False,
)
@option(
    "--verbose", "-v",
    type=BOOL,
    is_flag=True,
    required=False,
    default=False,
)
@argument(
    "dataset",
    type=STRING,
    required=True,
)
def main(
        host: str,
        username: str,
        password: str,
        meta_index: Optional[str],
        data_index: Optional[str],
        bucket: Optional[str],
        start: Optional[int],
        end: Optional[int],
        verbose: bool,
        dataset: str,
) -> None:
    if verbose:
        basicConfig()
        LOGGER.setLevel(INFO)

    export_file_path = Path('.metadata') / dataset_id.replace('/', '-') / 'index-metadata.yml'
    export_file_path.parent.mkdir(exist_ok=True, parents=True)

    with tracking(export_file_path=export_file_path, export_format=ExportFormat.IR_METADATA):
        index(
            es_host=host,
            es_username=username,
            es_password=password,
            es_index_meta=meta_index,
            es_index_data=data_index,
            s3_bucket=bucket,
            dataset_id=dataset,
            start=start,
            end=end,
        )


if __name__ == '__main__':
    main()
