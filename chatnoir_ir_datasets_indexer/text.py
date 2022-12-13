from chatnoir_ir_datasets_indexer import _PATTERN_REDUNDANT_WHITESPACE


def _collapse_whitespace(text: str) -> str:
    """Collapse white space and trim input string."""
    return _PATTERN_REDUNDANT_WHITESPACE.sub(" ", text).strip()
