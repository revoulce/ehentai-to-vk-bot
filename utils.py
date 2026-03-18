import re


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
