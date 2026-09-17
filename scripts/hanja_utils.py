"""Hanja ↔ Hangul conversion utilities with Korean initial-sound law (두성법칙).

Extracted from tag_tei_with_dict.py where this logic was duplicated in
``_clean_and_pre_tag_hanja`` and ``_normalize_person_candidate``.
"""

from __future__ import annotations

import hanja

# ── Korean Initial-Sound Law (두성법칙) ───────────────────────────────────
_CHOSUNG_LIST = [
    "ㄱ", "ㄲ", "ㄴ", "ㄷ", "ㄸ", "ㄹ", "ㅁ", "ㅂ", "ㅃ", "ㅅ",
    "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ",
]
_JUNGSUNG_Y_OR_I = {2, 3, 6, 7, 12, 17, 20}  # ㅑ, ㅒ, ㅕ, ㅖ, ㅛ, ㅠ, ㅣ


def apply_initial_sound_law(reading: str) -> str:
    """Apply the Korean initial-sound law (두성법칙) to *reading*.

    Decomposes the first Hangul syllable into initial/medial/final jamo:
    - Initial ㄹ:
      * Before ㅑ, ㅒ, ㅕ, ㅖ, ㅛ, ㅠ, ㅣ -> becomes ㅇ (e.g. 량->양, 력->역, 류->유, 리->이)
      * Before other vowels -> becomes ㄴ (e.g. 로->노, 론->논, 래->내, 락->낙)
    - Initial ㄴ:
      * Before ㅑ, ㅒ, ㅕ, ㅖ, ㅛ, ㅠ, ㅣ -> becomes ㅇ (e.g. 녀->여, 년->연, 뇨->요, 니->이)
    """
    if not reading or len(reading) < 2:
        return reading

    first_char = reading[0]
    code = ord(first_char)

    # Check if first character is a modern Hangul syllable (가-힣)
    if not (0xAC00 <= code <= 0xD7A3):
        return reading

    syllable_index = code - 0xAC00
    jong_index = syllable_index % 28
    jung_index = (syllable_index // 28) % 21
    cho_index = (syllable_index // 28) // 21

    new_cho_index = cho_index

    # Initial ㄹ (index 5)
    if cho_index == 5:
        if jung_index in _JUNGSUNG_Y_OR_I:
            new_cho_index = 11  # ㅇ
        else:
            new_cho_index = 2   # ㄴ
    # Initial ㄴ (index 2)
    elif cho_index == 2:
        if jung_index in _JUNGSUNG_Y_OR_I:
            new_cho_index = 11  # ㅇ

    if new_cho_index != cho_index:
        new_code = 0xAC00 + (new_cho_index * 21 + jung_index) * 28 + jong_index
        return chr(new_code) + reading[1:]

    return reading


# ── Specific Hanja-name overrides ──────────────────────────────────────────
_NAME_OVERRIDES: list[tuple[str, str, str]] = [
    # (hanja_prefix, wrong_reading_prefix, correct_reading_prefix)
    ("金", "금", "김"),
    ("丸山", "환산", "마루야마"),
    ("楠", "남", "구스노키"),
]


def translate_hanja_name(hanja_text: str) -> str:
    """Translate a Hanja string to Hangul, applying name overrides and 두성법칙.

    This covers the ``金`` → 김, ``丸山`` → 마루야마, ``楠`` → 구스노키
    special cases and then runs the standard initial-sound law.
    """
    reading = hanja.translate(hanja_text, "substitution")

    for prefix, wrong, correct in _NAME_OVERRIDES:
        if hanja_text.startswith(prefix) and reading.startswith(wrong):
            reading = reading.replace(wrong, correct, 1)

    return apply_initial_sound_law(reading)
