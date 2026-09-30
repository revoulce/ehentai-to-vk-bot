import re
from urllib.parse import urlsplit


def normalize_gallery_url(url: str) -> str:
    """Accept only gallery URLs and remove view parameters before deduplication."""
    if not isinstance(url, str):
        raise ValueError("URL must be a string")
    try:
        parsed = urlsplit(url.strip())
        valid_host = (
            parsed.hostname in {"e-hentai.org", "exhentai.org"}
            and parsed.port in {None, 80, 443}
            and parsed.username is None
            and parsed.password is None
        )
    except ValueError as exc:
        raise ValueError("Invalid gallery URL") from exc
    match = re.fullmatch(r"/g/([0-9]+)/([a-fA-F0-9]+)/?", parsed.path)
    if parsed.scheme not in {"http", "https"} or not valid_host or match is None:
        raise ValueError("Expected an E-Hentai or ExHentai gallery URL")
    gallery_id, token = match.groups()
    return f"https://{parsed.hostname}/g/{int(gallery_id)}/{token.lower()}/"


def clean_tag(tag_raw: str) -> str:
    """
    Converts 'alin ma' -> 'alin_ma', 'petra.fyed' -> 'petrafyed'.
    Removes non-alphanumeric chars except underscores.
    """
    cleaned = re.sub(r"['.\-!]", "", tag_raw)

    cleaned = cleaned.strip().replace(" ", "_")

    cleaned = re.sub(r"[^a-zA-Z0-9_а-яА-Я]", "", cleaned)

    return cleaned.lower()


def process_tags(raw_tags: list[str]) -> list[str]:
    """
    Handles splitting '|' and cleaning list of tags.
    Input: ["alin ma | xenon", "petra.fyed"]
    Output: ["#alin_ma", "#xenon", "#petrafyed"]
    """
    processed = []
    for raw in raw_tags:
        parts = raw.split("|")
        for part in parts:
            cleaned = clean_tag(part)
            if cleaned:
                processed.append(f"#{cleaned}")
    return processed
