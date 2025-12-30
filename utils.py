import re


def clean_tag(tag_raw: str) -> str:
    """
    Converts 'alin ma' -> 'alin_ma', 'petra.fyed' -> 'petrafyed'.
    Removes non-alphanumeric chars except underscores.
    """
    # 1. Remove specific chars that break hashtags but keep text semantic
    # Remove dots, dashes, apostrophes
    cleaned = re.sub(r"['.\-!]", "", tag_raw)

    # 2. Replace spaces with underscores
    cleaned = cleaned.strip().replace(" ", "_")

    # 3. Remove anything that isn't alphanumeric or underscore (safety net)
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
        # Split by pipe if exists
        parts = raw.split("|")
        for part in parts:
            cleaned = clean_tag(part)
            if cleaned:
                processed.append(f"#{cleaned}")
    return processed
