from re import compile

_PATTERN_REDUNDANT_WHITESPACE = compile(r"\s{2,}")


def collapse_whitespace(text: str) -> str:
    """Collapse white space and trim input string."""
    return _PATTERN_REDUNDANT_WHITESPACE.sub(" ", text).strip()
