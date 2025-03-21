from typing import Mapping


def index_action(
        doc_id: str,
        index: str,
        data: Mapping[str, str],
) -> Mapping[str, str]:
    return {
        '_op_type': 'index',
        '_index': index,
        '_id': doc_id,
        **data
    }
