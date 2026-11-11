"""The third arm: the gated prompt's guidance, without the structure that enforces it.

Derived from agent/diagnosis.py rather than copied, so it can only differ in the ways
listed here:

  - the response schema has no `rootCause` field, so the model never commits to where
    the problem lives before it writes the patch;
  - the prompt's root-cause definitions are kept word for word, but framed as a decision
    to make before patching instead of a field to fill in;
  - no server-side gate on the root cause (see gates.prompt_only_blocker).

Everything else — the four definitions, "never make code tolerate bad input", the
migration rules — is the gated prompt verbatim. Comparing this arm with the other two
separates what the instructions do from what the schema field and the gate add.
"""

import copy

from agent import diagnosis

# (gated wording, prompt-only wording). Each must occur exactly once in the live prompt,
# so a later edit to diagnosis.SYSTEM breaks this module loudly instead of silently.
_REWORDINGS = [
    ("- rootCause: where the problem actually lives.\n",
     "- Before proposing any patch, decide where the problem actually lives:\n"),
    ("- patch: only when rootCause is code or config:",
     "- patch: only when the problem lives in code or config:"),
]


def _derive_system(gated: str) -> str:
    text = gated
    for old, new in _REWORDINGS:
        if text.count(old) != 1:
            raise RuntimeError(f"agent/diagnosis.py SYSTEM changed; expected one {old!r}")
        text = text.replace(old, new)
    if "rootCause" in text:
        raise RuntimeError("the prompt-only arm still mentions the rootCause field")
    return text


def _derive_schema(gated: dict) -> dict:
    schema = copy.deepcopy(gated)
    del schema["properties"]["rootCause"]
    schema["required"] = [f for f in schema["required"] if f != "rootCause"]
    return schema


SYSTEM = _derive_system(diagnosis.SYSTEM)
FIX_SCHEMA = _derive_schema(diagnosis.FIX_SCHEMA)
