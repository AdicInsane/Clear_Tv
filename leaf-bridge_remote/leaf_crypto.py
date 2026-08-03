"""
leaf_crypto.py - Python port of SKLiveCryptoUtils.java (from SKTechProvider.cs3)

Full implementation of the V25 + V23 + Legacy decryption chain.
Reverse-engineered from the compiled CloudStream plugin (SKTechProvider.cs3).

Keys extracted from SKTechProvider.cs3 static initialiser:
  V25_KEY1 = "V9LQR42pNKc7smaX"
  V25_KEY2 = "d2WT1lR4ckEvUsdk"
  V25_IV   = "I=4dkwPiOuROD+pD"
  V23_KEY  = "ST00ZGt3UGlPdVJP"   (base64 of "ST00ZGt3UGlPdVJP")
  V23_IV   = "d2WT1lR4ckEvUsdk"
  LEGACY_AES_KEY = hex "6c326c356b4237784335715031724b31"
  LEGACY_AES_IV  = hex "70314b356e50377542386848316c3139"

decryptSKLive() tries in order:
  1. preprocessResponse() → decryptV25Pass1  (V25_KEY1 as IV)
  2. preprocessResponse() → decryptV25Pass2  (V25_KEY2 as IV)
  3. decryptV23()
  4. decryptLegacy() on preprocessed input
  5. decryptLegacy() on base64-unwrapped input
  6. decryptLegacy() on raw input
"""

from __future__ import annotations

import base64
import logging

logger = logging.getLogger("leaf.crypto")

# ── All keys (from SKTechProvider.cs3 static block) ──────────────────────────
_V25_KEY1 = b"V9LQR42pNKc7smaX"
_V25_KEY2 = b"d2WT1lR4ckEvUsdk"
_V25_IV   = b"I=4dkwPiOuROD+pD"
_V23_KEY  = b"ST00ZGt3UGlPdVJP"
_V23_IV   = b"d2WT1lR4ckEvUsdk"
_LEGACY_AES_KEY = bytes.fromhex("6c326c356b4237784335715031724b31")
_LEGACY_AES_IV  = bytes.fromhex("70314b356e50377542386848316c3139")

# Sentinel suffix used by preprocessResponse to validate the output
_PREPROCESS_SENTINEL = "BA@GBA@GBA@GBA@G"

# ── Lookup table for legacy alphabet substitution ─────────────────────────────
_LOOKUP_TABLE_D = (
    "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
    "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
    " !\"#$%&'()*+,-./"
    "0123456789:;<=>?"
    "@EGMNKABUVCDYHLI"
    "FPOZQSRWTXJ[\\]^_"
    "`egmnkabuvcdyhli"
    "fpozqsrwtxj{|}~\x7f"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pad_base64(s: str) -> str:
    rem = len(s) % 4
    return s + "=" * (4 - rem) if rem else s


def _aes_cbc_decrypt(ciphertext: bytes, key: bytes, iv: bytes) -> bytes | None:
    """AES/CBC/PKCS5 decrypt. Returns plaintext or None."""
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
        if len(ciphertext) % 16 != 0:
            return None
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return unpad(cipher.decrypt(ciphertext), 16)
    except Exception:
        return None


def _aes_decrypt_and_transform(ciphertext: bytes, key: bytes, iv: bytes) -> bytes | None:
    """
    AES/CBC decrypt then swap adjacent byte pairs then reverse.
    This is the V25 post-decrypt transform (aesDecryptAndTransform in Java).
    """
    plain = _aes_cbc_decrypt(ciphertext, key, iv)
    if plain is None:
        return None
    arr = bytearray(plain)
    # swap adjacent pairs
    for i in range(0, len(arr) - 1, 2):
        arr[i], arr[i + 1] = arr[i + 1], arr[i]
    arr.reverse()
    return bytes(arr)


def _prepare_ciphertext(encrypted_data: str) -> bytes | None:
    """
    prepareCiphertext() from Java:
    - If starts with "==" → reverse the whole string first
    - base64-decode
    - If len % 16 == 12 → strip first 12 bytes (header)
    - If len % 16 == 0  → use as-is
    - Otherwise → None
    """
    src = encrypted_data[::-1] if encrypted_data.startswith("==") else encrypted_data
    try:
        decoded = base64.b64decode(_pad_base64(src))
    except Exception:
        return None
    if len(decoded) > 12 and len(decoded) % 16 == 12:
        return decoded[12:]
    if len(decoded) % 16 == 0:
        return decoded
    return None


def _is_json(text: str) -> bool:
    """Return True if text looks like a JSON array or object."""
    stripped = text.lstrip()
    return stripped.startswith("[") or stripped.startswith("{")


def _custom_to_standard_base64(custom_b64: str) -> str:
    result = []
    for ch in custom_b64:
        code = ord(ch)
        if code < len(_LOOKUP_TABLE_D):
            result.append(_LOOKUP_TABLE_D[code])
        else:
            result.append(ch)
    return "".join(result)


# ── Individual decrypt methods ────────────────────────────────────────────────

def _preprocess_response(raw: str) -> str | None:
    """
    preprocessResponse() from Java:
    1. Swap adjacent char pairs in the raw string
    2. Reverse the result
    3. base64-decode
    4. Check it ends with sentinel "BA@GBA@GBA@GBA@G"
    5. Strip the sentinel and return
    """
    try:
        chars = list(raw)
        for i in range(0, len(chars) - 1, 2):
            chars[i], chars[i + 1] = chars[i + 1], chars[i]
        reversed_str = "".join(chars)[::-1]
        decoded = base64.b64decode(reversed_str)
        text = decoded.decode("utf-8")
        if not text.endswith(_PREPROCESS_SENTINEL):
            return None
        return text[: -len(_PREPROCESS_SENTINEL)]
    except Exception:
        return None


def _decrypt_v25_pass1(encrypted_data: str) -> str | None:
    """V25 Pass 1: prepareCiphertext → aesDecryptAndTransform(V23_KEY, V25_KEY1) → base64decode → JSON check."""
    try:
        ct = _prepare_ciphertext(encrypted_data)
        if ct is None:
            return None
        plain = _aes_decrypt_and_transform(ct, _V23_KEY, _V25_KEY1)
        if plain is None:
            return None
        decoded = base64.b64decode(plain)
        text = decoded.decode("utf-8")
        return text if _is_json(text) else None
    except Exception:
        return None


def _decrypt_v25_pass2(encrypted_data: str) -> str | None:
    """V25 Pass 2: prepareCiphertext → aesDecryptAndTransform(V23_KEY, V25_KEY2) → base64decode → JSON check."""
    try:
        ct = _prepare_ciphertext(encrypted_data)
        if ct is None:
            return None
        plain = _aes_decrypt_and_transform(ct, _V23_KEY, _V25_KEY2)
        if plain is None:
            return None
        decoded = base64.b64decode(plain)
        text = decoded.decode("utf-8")
        return text if _is_json(text) else None
    except Exception:
        return None


def _decrypt_v23(encrypted_data: str) -> str | None:
    """
    V23: pad → base64decode → swap pairs → reverse → base64decode → AES(V23_KEY, V23_IV) → JSON check.
    (Original V23 algorithm, unchanged from old format.)
    """
    try:
        inner = bytearray(base64.b64decode(_pad_base64(encrypted_data)))
        for i in range(0, len(inner) - 1, 2):
            inner[i], inner[i + 1] = inner[i + 1], inner[i]
        inner.reverse()
        ct = base64.b64decode(bytes(inner))
        plain = _aes_cbc_decrypt(ct, _V23_KEY, _V23_IV)
        if plain is None:
            return None
        text = plain.decode("utf-8")
        return text if _is_json(text) else None
    except Exception:
        return None


def _decrypt_legacy(encrypted_data: str) -> str | None:
    """Legacy: custom alphabet substitution → base64decode → AES(LEGACY_KEY, LEGACY_IV)."""
    try:
        standard_b64 = _custom_to_standard_base64(encrypted_data)
        ct = base64.b64decode(standard_b64)
        plain = _aes_cbc_decrypt(ct, _LEGACY_AES_KEY, _LEGACY_AES_IV)
        if plain is None:
            return None
        return plain.decode("utf-8")
    except Exception:
        return None


# ── Public entry point ────────────────────────────────────────────────────────

def decrypt_sklive(encrypted_data: str) -> str | None:
    """
    Full decryption chain matching decryptSKLive() in SKLiveCryptoUtils.java.
    Tries all methods in order, returns first success or None.
    """
    data = encrypted_data.strip()

    # Step 1: preprocess (swap chars + reverse + b64 + sentinel check)
    preprocessed = _preprocess_response(data)
    v25_input = preprocessed if preprocessed is not None else data

    # Step 2: V25 Pass 1
    result = _decrypt_v25_pass1(v25_input)
    if result is not None:
        logger.debug("decrypt_sklive: V25 Pass1 succeeded")
        return result

    # Step 3: V25 Pass 2
    result = _decrypt_v25_pass2(v25_input)
    if result is not None:
        logger.debug("decrypt_sklive: V25 Pass2 succeeded")
        return result

    # Step 4: V23 (on original data)
    result = _decrypt_v23(data)
    if result is not None:
        logger.debug("decrypt_sklive: V23 succeeded")
        return result

    # Step 5: Legacy on preprocessed/v25_input
    result = _decrypt_legacy(v25_input)
    if result is not None:
        logger.debug("decrypt_sklive: Legacy (preprocessed) succeeded")
        return result

    # Step 6: Legacy on base64-unwrapped input
    try:
        legacy_input = base64.b64decode(data).decode("utf-8")
    except Exception:
        legacy_input = data

    result = _decrypt_legacy(legacy_input)
    if result is not None:
        logger.debug("decrypt_sklive: Legacy (b64-unwrapped) succeeded")
        return result

    # Step 7: Legacy on raw input (if different from unwrapped)
    if legacy_input != data:
        result = _decrypt_legacy(data)
        if result is not None:
            logger.debug("decrypt_sklive: Legacy (raw) succeeded")
            return result

    logger.warning("decrypt_sklive: all methods failed")
    return None
