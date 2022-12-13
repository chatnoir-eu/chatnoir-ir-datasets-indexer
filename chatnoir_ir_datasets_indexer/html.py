from typing import Sequence

from resiliparse.parse.html import HTMLTree

from chatnoir_ir_datasets_indexer.text import collapse_whitespace


def extract_title(html_tree: HTMLTree) -> str:
    """
    Heuristically extract a document title from an HTML tree.
    """
    title = html_tree.title
    if title is not None:
        title = collapse_whitespace(title)
    if title is not None and len(title) > 0:
        return title

    for selector in ("h1", "h2", ".title"):
        element = html_tree.body.query_selector(selector)
        if element is None:
            continue
        title = element.text
        if title is None:
            continue
        title = collapse_whitespace(title)
        if title is None or len(title) == 0:
            continue
        return title
    return ""


def extract_meta_description(html_tree: HTMLTree) -> str:
    """
    Extract a document meta description from an HTML tree.
    """
    if html_tree.head is None:
        return ""

    element = html_tree.head.query_selector("meta[name=\"description\"]")
    if element is None:
        return ''

    return collapse_whitespace(element.getattr('content', ''))


def extract_meta_keywords(
        html_tree: HTMLTree,
        max_keyword_length: int = 80,
        max_keywords: int = 30,
) -> Sequence[str]:
    """
    Extract deduplicated and lower-cased document meta keywords
    from an HTML tree in their original order.
    :param max_keyword_length: Maximum length of individual keywords.
     Keywords are cut off after the maximum length.
    :param max_keywords: Maximum number of keywords to return.
    """
    if html_tree.head is None:
        return []

    element = html_tree.head.query_selector("meta[name=\"keywords\"]")
    if element is None:
        return []

    keywords = [
        collapse_whitespace(keyword)[:max_keyword_length].lower()
        for keyword in element.getattr("content", "").split(",")
    ]
    unique_keywords = []
    for keyword in keywords:
        if keyword not in unique_keywords:
            unique_keywords.append(keyword)
        if len(unique_keywords) >= max_keywords:
            break
    return unique_keywords


def extract_headings(
        html_tree: HTMLTree,
        max_level: int = 3,
) -> Sequence[str]:
    """
    Extract non-empty document headings up to a certain level
    from an HTML tree.
    :param max_level: Maximum heading level to extract.
    """
    if html_tree.head is None:
        return []

    selectors = [
        f"h{level}"
        for level in range(1, max_level + 1)
    ]
    selector = ", ".join(selectors)

    elements = html_tree.head.query_selector_all(selector)
    headings = (
        collapse_whitespace(element.text)
        for element in elements
        if element.text is not None
    )
    return [
        heading
        for heading in headings
        if len(heading) > 0
    ]
