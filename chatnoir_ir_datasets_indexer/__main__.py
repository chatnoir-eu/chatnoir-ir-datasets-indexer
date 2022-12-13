from typing import Optional

from click import option, argument, STRING, INT, command, BOOL

from chatnoir_ir_datasets_indexer.index import index

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
    required=False,
    default=False,
)
@argument(
    "dataset",
    type=STRING,
    required=True,
)
def main(
        es_host: str,
        es_username: str,
        es_password: str,
        es_meta_index: Optional[str],
        es_data_index: Optional[str],
        s3_bucket: Optional[str],
        start: Optional[int],
        end: Optional[int],
        verbose: bool,
        dataset: str,
) -> None:
    index(
        es_host=es_host,
        es_username=es_username,
        es_password=es_password,
        es_index_meta=es_meta_index,
        es_index_data=es_data_index,
        s3_bucket=s3_bucket,
        dataset_id=dataset,
        start=start,
        end=end,
        verbose=verbose,
    )


if __name__ == '__main__':
    main()
