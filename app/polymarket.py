"""
Polymarket CLOB V2 integration — adapted from the provided executor.
Handles market discovery, shadow/live order placement, balance, and resolution.
"""
from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from common locations (EC2/Docker/systemd cwd varies)
_APP_DIR = Path(__file__).resolve().parent
_ROOT = _APP_DIR.parent
for _env_path in (
    _ROOT / ".env",
    Path.cwd() / ".env",
    Path("/app/.env"),
    Path("/home/ubuntu/.env"),
    Path("/home/ubuntu/polymarket-strategy/.env"),
    Path("/home/ec2-user/polymarket-dual/.env"),
    Path("/home/ec2-user/polymarket-strategy/.env"),
    Path("/home/ubuntu/polymarket-bot/.env"),
):
    if _env_path.is_file():
        load_dotenv(_env_path, override=False)
load_dotenv(override=False)  # cwd fallback

import time
import json
import hmac
import hashlib
import base64
import logging
import threading
import random
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"
# FAK/FOK market-order dollar floor (not GTC share min). See attached executor.
MARKET_ORDER_MIN_USD = 1.0
GAMMA_BASE = "https://gamma-api.polymarket.com"
WWW_BASE = "https://polymarket.com"
CHAIN_ID = 137
PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
ZERO_BYTES32 = "0x" + "00" * 32
_MAX_UINT = 2**256 - 1

_PROXY_ENV = "POLYMARKET_PROXY"
_l2_cache: dict = {}
_l2_cache_lock = threading.Lock()
_confirmed_sig_type: Optional[int] = None

# Per-thread credential override for multi-user independent execution
_creds_local = threading.local()


def push_user_creds(creds: Optional[dict]) -> None:
    """creds keys: private_key, api_key, api_secret, api_passphrase, funder, signature_type"""
    _creds_local.creds = creds


def pop_user_creds() -> None:
    _creds_local.creds = None


def _override() -> dict:
    return getattr(_creds_local, "creds", None) or {}


def _proxies():
    raw = os.environ.get(_PROXY_ENV, "").strip()
    if not raw:
        return None
    parts = raw.split(":", 3)
    if len(parts) != 4:
        return None
    host, port, user, password = parts
    url = f"http://{user}:{password}@{host}:{port}"
    return {"http": url, "https": url}


def get_private_key() -> Optional[str]:
    ov = _override()
    if ov.get("private_key"):
        pk = str(ov["private_key"]).strip().strip('"').strip("'")
        if pk:
            return pk if pk.startswith("0x") else "0x" + pk
    pk = (
        os.environ.get("POLYMARKET_PRIVATE_KEY")
        or os.environ.get("PRIVATE_KEY")
        or ""
    ).strip().strip('"').strip("'")
    if not pk:
        return None
    return pk if pk.startswith("0x") else "0x" + pk



def get_signer_address() -> Optional[str]:
    pk = get_private_key()
    if not pk:
        return None
    try:
        from eth_account import Account
        from web3 import Web3
        return Web3.to_checksum_address(Account.from_key(pk).address)
    except Exception:
        return None


def get_funder_address() -> Optional[str]:
    ov = _override()
    if ov.get("funder"):
        raw = str(ov["funder"]).strip().strip('"').strip("'")
        if raw:
            try:
                from web3 import Web3
                return Web3.to_checksum_address(raw)
            except Exception:
                return raw
    explicit = (
        os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        or os.environ.get("POLYMARKET_DEPOSIT_WALLET", "")
        or ""
    ).strip().strip('"').strip("'")
    if explicit:
        try:
            from web3 import Web3
            return Web3.to_checksum_address(explicit)
        except Exception:
            return explicit
    return get_signer_address()



def get_deposit_wallet_address() -> Optional[str]:
    """
    Deposit / portfolio wallet (maker). Same as funder for proxy (1) and deposit (3).
    Optional POLYMARKET_DEPOSIT_WALLET overrides funder when they differ.
    """
    ov = _override()
    if ov.get("deposit_wallet"):
        raw = str(ov["deposit_wallet"]).strip()
        if raw:
            try:
                from web3 import Web3
                return Web3.to_checksum_address(raw)
            except Exception:
                return raw
    explicit = os.environ.get("POLYMARKET_DEPOSIT_WALLET", "").strip().strip('"').strip("'")
    if explicit:
        try:
            from web3 import Web3
            return Web3.to_checksum_address(explicit)
        except Exception:
            return explicit
    return get_funder_address()


def get_signature_type() -> int:
    """0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE, 3=POLY_1271 deposit wallet.

    Session-key model (private key address != funder): always use 1 (POLY_PROXY).
    Type 3 alone fails with signer/API-key mismatch for session EOAs.
    """
    ov = _override()
    # Connected user: session EOA + different funder → deposit wallet type 3
    try:
        pk = ov.get("private_key") if ov else None
        funder = (ov.get("funder") or "").strip().lower() if ov else ""
        if pk and funder:
            from eth_account import Account
            from web3 import Web3
            addr = Web3.to_checksum_address(Account.from_key(
                pk if str(pk).startswith("0x") else "0x" + str(pk)
            ).address).lower()
            if addr and funder and addr != funder:
                return 3
    except Exception:
        pass
    if ov and ov.get("signature_type") is not None:
        try:
            v = int(ov["signature_type"])
            if v in (0, 1, 2, 3):
                return v
        except Exception:
            pass
    raw = os.environ.get("POLYMARKET_SIGNATURE_TYPE", "").strip()
    if raw == "":
        return 1
    try:
        v = int(raw)
        return v if v in (0, 1, 2, 3) else 1
    except ValueError:
        return 1


_CLOB_AUTH_TYPES = [
    {"name": "address", "type": "address"},
    {"name": "timestamp", "type": "string"},
    {"name": "nonce", "type": "uint256"},
    {"name": "message", "type": "string"},
]
_CLOB_AUTH_MESSAGE = "This message attests that I control the given wallet"




def get_builder_api_key() -> Optional[str]:
    return (
        os.environ.get("POLYMARKET_BUILDER_API_KEY", "").strip()
        or os.environ.get("BUILDER_API_KEY", "").strip()
        or None
    )


def get_builder_secret() -> Optional[str]:
    return (
        os.environ.get("POLYMARKET_BUILDER_SECRET", "").strip()
        or os.environ.get("POLYMARKET_BUILDER_API_SECRET", "").strip()
        or os.environ.get("BUILDER_SECRET", "").strip()
        or None
    )


def get_builder_passphrase() -> Optional[str]:
    return (
        os.environ.get("POLYMARKET_BUILDER_PASSPHRASE", "").strip()
        or os.environ.get("BUILDER_PASSPHRASE", "").strip()
        or None
    )



def get_builder_code() -> Optional[str]:
    """
    Public bytes32 builder code for order attribution (leaderboard credit).
    Copy from polymarket.com → Settings → Builders → Builder Code.
    Env: POLYMARKET_BUILDER_CODE or POLY_BUILDER_CODE
    """
    raw = (
        os.environ.get("POLYMARKET_BUILDER_CODE", "")
        or os.environ.get("POLY_BUILDER_CODE", "")
        or ""
    ).strip().strip('"').strip("'")
    if not raw or raw.lower() in ("none", "off", "0"):
        return None
    if not raw.startswith("0x"):
        raw = "0x" + raw
    # normalize to 32 bytes hex
    hexpart = raw[2:]
    if len(hexpart) > 64:
        hexpart = hexpart[-64:]
    elif len(hexpart) < 64:
        hexpart = hexpart.zfill(64)
    return "0x" + hexpart.lower()


def builder_configured() -> bool:
    return bool(get_builder_api_key() and get_builder_secret() and get_builder_passphrase())


def _builder_hmac_signature(secret: str, timestamp: str, method: str, path: str, body: str = "") -> str:
    """
    POLY_BUILDER_SIGNATURE = base64(HMAC-SHA256(secret, timestamp + method + path + body))
    Matches Polymarket builder-signing-sdk / docs remote builder signing.
    """
    import base64
    import hashlib
    import hmac
    message = f"{timestamp}{method}{path}{body or ''}"
    # secret may be raw or base64
    try:
        key = base64.urlsafe_b64decode(secret)
    except Exception:
        try:
            key = base64.b64decode(secret)
        except Exception:
            key = secret.encode() if isinstance(secret, str) else secret
    dig = hmac.new(key, message.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(dig).decode()


def builder_auth_headers(method: str, path: str, body: str = "") -> dict:
    """Headers for Relayer / Builder-authenticated endpoints."""
    key = get_builder_api_key()
    secret = get_builder_secret()
    phrase = get_builder_passphrase()
    if not (key and secret and phrase):
        raise ValueError("Builder API key/secret/passphrase not configured")
    ts = str(int(time.time()))
    sig = _builder_hmac_signature(secret, ts, method.upper(), path, body)
    return {
        "Content-Type": "application/json",
        "POLY_BUILDER_API_KEY": key,
        "POLY_BUILDER_TIMESTAMP": ts,
        "POLY_BUILDER_PASSPHRASE": phrase,
        "POLY_BUILDER_SIGNATURE": sig,
    }


def authorize_session_key(
    *,
    deposit_wallet: str,
    session_address: str,
    scopes: Optional[list] = None,
    valid_days: int = 180,
) -> dict:
    """
    Authorize a Session Key on a Deposit Wallet via Builder + Relayer.
    POST https://relayer-v2.polymarket.com/v1/session-signers/authorizations

    Requires POLYMARKET_BUILDER_API_KEY / SECRET / PASSPHRASE.
    For wallets the builder controls (or when Polymarket accepts builder auth for
    that wallet), this completes authorization without the user's main private key.
    """
    if not builder_configured():
        return {
            "success": False,
            "error": "Builder API not configured. Set POLYMARKET_BUILDER_API_KEY, "
                     "POLYMARKET_BUILDER_SECRET, POLYMARKET_BUILDER_PASSPHRASE",
        }
    if not deposit_wallet or not session_address:
        return {"success": False, "error": "deposit_wallet and session_address required"}

    scopes = scopes or ["CLOB"]
    valid_until = int(time.time()) + int(valid_days) * 86400
    path = "/v1/session-signers/authorizations"
    payload = {
        "walletAddress": deposit_wallet,
        "sessionSignerAddress": session_address,
        "scopes": scopes,
        "validUntil": str(valid_until),
    }
    body = json.dumps(payload, separators=(",", ":"))
    try:
        headers = builder_auth_headers("POST", path, body)
        headers["Idempotency-Key"] = f"sess-{session_address[:16]}-{valid_until}"
        resp = requests.post(
            f"https://relayer-v2.polymarket.com{path}",
            headers=headers,
            data=body,
            timeout=30,
            proxies=_proxies(),
        )
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:500]}
        if resp.ok:
            logger.info(
                "[BUILDER] session authorized wallet=%s session=%s status=%s",
                deposit_wallet, session_address, resp.status_code,
            )
            return {"success": True, "data": data, "valid_until": valid_until, "scopes": scopes}
        err = data.get("error") or data.get("message") or resp.text[:300]
        logger.warning("[BUILDER] authorize failed %s: %s", resp.status_code, err)
        return {"success": False, "error": f"HTTP {resp.status_code}: {err}", "status_code": resp.status_code}
    except Exception as e:
        logger.exception("[BUILDER] authorize_session_key")
        return {"success": False, "error": str(e)}


# Deposit wallet factory (Polygon mainnet) — WALLET-CREATE target
_DEPOSIT_WALLET_FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
_DEPOSIT_WALLET_IMPL = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB"
_DEPOSIT_WALLET_BEACON = "0x7A18EDfe055488A3128f01F563e5B479D92ffc3a"
_RELAYER_BASE = "https://relayer-v2.polymarket.com"


def _keccak(data: bytes) -> bytes:
    try:
        from eth_utils import keccak
        return keccak(data)
    except Exception:
        try:
            from Crypto.Hash import keccak as kc
            k = kc.new(digest_bits=256)
            k.update(data)
            return k.digest()
        except Exception:
            from web3 import Web3
            return bytes(Web3.keccak(data))


def derive_deposit_wallet_addresses(owner_address: str) -> dict:
    """
    Deterministic CREATE2 addresses for an owner (UUPS + Beacon shapes).
    Matches Polymarket builder-relayer-client / Solady LibClone.
    """
    owner = (owner_address or "").strip()
    if not owner.startswith("0x"):
        owner = "0x" + owner
    factory = bytes.fromhex(_DEPOSIT_WALLET_FACTORY[2:])
    impl = bytes.fromhex(_DEPOSIT_WALLET_IMPL[2:])
    beacon = bytes.fromhex(_DEPOSIT_WALLET_BEACON[2:])
    owner_b = bytes.fromhex(owner[2:].lower().zfill(40))

    wallet_id = owner_b.rjust(32, b"\x00")
    args = factory.rjust(32, b"\x00") + wallet_id
    salt = _keccak(args)

    # UUPS ERC1967
    c1 = bytes.fromhex("cc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3")
    c2 = bytes.fromhex("5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076")
    prefix = int.from_bytes(bytes.fromhex("61003d3d8160233d3973"), "big")
    n = len(args)
    combined = (prefix + (n << 56)).to_bytes(10, "big")
    uups_code = combined + impl + bytes.fromhex("6009") + c2 + c1 + args
    uups_hash = _keccak(uups_code)
    uups = "0x" + _keccak(b"\xff" + factory + salt + uups_hash)[-20:].hex()

    # Beacon ERC1967
    bc1 = bytes.fromhex("b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3")
    bc2 = bytes.fromhex("1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c")
    bc3 = bytes.fromhex("60195155f3363d3d373d3d363d602036600436635c60da")
    bprefix = int.from_bytes(bytes.fromhex("6100523d8160233d3973"), "big")
    bcombined = (bprefix + (n << 56)).to_bytes(10, "big")
    beacon_code = bcombined + beacon + bc3 + bc2 + bc1 + args
    beacon_hash = _keccak(beacon_code)
    beacon_addr = "0x" + _keccak(b"\xff" + factory + salt + beacon_hash)[-20:].hex()

    try:
        from web3 import Web3
        uups = Web3.to_checksum_address(uups)
        beacon_addr = Web3.to_checksum_address(beacon_addr)
    except Exception:
        pass

    return {"uups": uups, "beacon": beacon_addr, "owner": owner, "factory": _DEPOSIT_WALLET_FACTORY}


def resolve_deposit_wallet(owner_address: str) -> Optional[str]:
    """Prefer on-chain deployed address (beacon then UUPS); else beacon (post-2026 default)."""
    addrs = derive_deposit_wallet_addresses(owner_address)
    rpc = (
        os.environ.get("POLYGON_RPC_URL")
        or os.environ.get("RPC_URL")
        or "https://polygon-rpc.com"
    )
    for key in ("beacon", "uups"):
        addr = addrs.get(key)
        if not addr:
            continue
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getCode",
                "params": [addr, "latest"],
            }
            r = requests.post(rpc, json=payload, timeout=12, proxies=_proxies())
            code = (r.json() or {}).get("result") or "0x"
            if code and code != "0x" and len(code) > 4:
                logger.info("[BUILDER] resolved deposit %s via %s for owner %s", addr, key, owner_address)
                return addr
        except Exception as e:
            logger.debug("resolve deposit code check %s: %s", key, e)
    # Default: beacon (new wallets)
    return addrs.get("beacon") or addrs.get("uups")


def deploy_deposit_wallet(owner_address: str) -> dict:
    """
    Deploy a deposit wallet for owner_address via Builder WALLET-CREATE.
    Owner controls the wallet; Builder only deploys gaslessly.
    Does NOT use operator POLYMARKET_PRIVATE_KEY for user wallets.
    """
    if not builder_configured():
        return {"success": False, "error": "Builder API not configured"}
    owner = (owner_address or "").strip()
    if not owner.startswith("0x") or len(owner) < 42:
        return {"success": False, "error": "Invalid owner address"}

    path = "/submit"
    payload = {
        "type": "WALLET-CREATE",
        "from": owner,
        "to": _DEPOSIT_WALLET_FACTORY,
        "metadata": "Deploy Deposit Wallet",
    }
    body = json.dumps(payload, separators=(",", ":"))
    try:
        headers = builder_auth_headers("POST", path, body)
        resp = requests.post(
            f"{_RELAYER_BASE}{path}",
            headers=headers,
            data=body,
            timeout=45,
            proxies=_proxies(),
        )
        def _as_dict(obj):
            """Relayer sometimes returns a list of tx objects — normalize to dict."""
            if isinstance(obj, dict):
                return obj
            if isinstance(obj, list) and obj:
                first = obj[0]
                return first if isinstance(first, dict) else {"items": obj}
            if isinstance(obj, list):
                return {}
            return {"raw": str(obj)[:500]}

        try:
            data = _as_dict(resp.json())
        except Exception:
            data = {"raw": resp.text[:500]}
        if not resp.ok:
            err = data.get("error") or data.get("message") or resp.text[:300]
            err_l = str(err).lower()
            # Wallet already deployed for this owner — resolve address and continue
            if "already exists" in err_l or "already deployed" in err_l:
                deposit = resolve_deposit_wallet(owner)
                logger.info(
                    "[BUILDER] WALLET-CREATE already exists owner=%s deposit=%s",
                    owner, deposit,
                )
                return {
                    "success": True,
                    "owner": owner,
                    "deposit_wallet": deposit,
                    "transaction_id": None,
                    "state": "ALREADY_EXISTS",
                    "data": data,
                    "note": "Deposit wallet already exists for this owner — using derived address.",
                }
            logger.warning("[BUILDER] WALLET-CREATE failed %s: %s", resp.status_code, err)
            return {"success": False, "error": f"HTTP {resp.status_code}: {err}", "data": data}

        tx_id = (
            data.get("transactionID")
            or data.get("transactionId")
            or data.get("transaction_id")
            or data.get("id")
        )
        logger.info("[BUILDER] WALLET-CREATE submitted owner=%s tx=%s data_keys=%s", owner, tx_id, list(data.keys())[:12])

        deposit = (
            data.get("proxyAddress")
            or data.get("proxy_address")
            or data.get("walletAddress")
            or data.get("wallet_address")
            or data.get("depositWallet")
        )
        state = data.get("state") or data.get("status")

        if tx_id:
            for _ in range(20):
                time.sleep(2.0)
                try:
                    pr = requests.get(
                        f"{_RELAYER_BASE}/transaction",
                        params={"id": tx_id},
                        headers=builder_auth_headers("GET", f"/transaction?id={tx_id}", ""),
                        timeout=20,
                        proxies=_proxies(),
                    )
                    if not pr.ok:
                        # Alternate poll path
                        pr2 = requests.get(
                            f"{_RELAYER_BASE}/v1/account/transactions/{tx_id}",
                            headers=builder_auth_headers("GET", f"/v1/account/transactions/{tx_id}", ""),
                            timeout=20,
                            proxies=_proxies(),
                        )
                        pd = _as_dict(pr2.json()) if pr2.ok else {}
                    else:
                        pd = _as_dict(pr.json())
                except Exception as e:
                    logger.debug("poll deploy: %s", e)
                    continue
                state = pd.get("state") or pd.get("status") or state
                nested = pd.get("data") if isinstance(pd.get("data"), dict) else {}
                cand = (
                    pd.get("proxyAddress")
                    or pd.get("proxy_address")
                    or pd.get("walletAddress")
                    or pd.get("wallet_address")
                    or pd.get("depositWallet")
                    or nested.get("proxyAddress")
                    or nested.get("walletAddress")
                )
                if cand and str(cand).startswith("0x") and len(str(cand)) >= 42:
                    if str(cand).lower() != _DEPOSIT_WALLET_FACTORY.lower():
                        deposit = str(cand)
                logger.info(
                    "[BUILDER] poll tx=%s state=%s deposit=%s keys=%s",
                    tx_id, state, deposit, list(pd.keys())[:10],
                )
                if state in ("STATE_CONFIRMED", "CONFIRMED", "STATE_MINED", "STATE_EXECUTED") and deposit:
                    break
                if state in ("STATE_CONFIRMED", "CONFIRMED", "STATE_MINED", "STATE_EXECUTED"):
                    break
                if state in ("STATE_FAILED", "STATE_INVALID", "FAILED"):
                    return {
                        "success": False,
                        "error": f"Deploy failed state={state}: {pd.get('error_msg') or pd}",
                        "transaction_id": tx_id,
                    }

        return {
            "success": True,
            "owner": owner,
            "deposit_wallet": deposit,
            "transaction_id": tx_id,
            "state": state,
            "data": data,
            "note": (
                "Deposit wallet deploy submitted."
                if deposit
                else "Deploy submitted; deposit address not in poll response yet — re-run Setup managed wallet."
            ),
        }
    except Exception as e:
        logger.exception("[BUILDER] deploy_deposit_wallet")
        return {"success": False, "error": str(e)}


def authorize_session_key_with_owner(
    *,
    owner_private_key: str,
    deposit_wallet: str,
    session_address: str,
    scopes: Optional[list] = None,
    valid_days: int = 180,
) -> dict:
    """
    Full session authorize: owner signs DepositWallet Batch + Builder HMAC.
    Required by Polymarket when simple Builder-only authorize returns 403.
    """
    if not builder_configured():
        return {"success": False, "error": "Builder API not configured"}
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    from web3 import Web3

    try:
        pk = owner_private_key if str(owner_private_key).startswith("0x") else "0x" + str(owner_private_key)
        owner_acct = Account.from_key(pk)
        owner_addr = Web3.to_checksum_address(owner_acct.address)
        deposit = Web3.to_checksum_address(deposit_wallet)
        session = Web3.to_checksum_address(session_address)
    except Exception as e:
        return {"success": False, "error": f"Invalid key/address: {e}"}

    scopes = scopes or ["CLOB"]
    valid_until = int(time.time()) + int(valid_days) * 86400
    deadline = int(time.time()) + 600

    # Wallet nonce for WALLET type
    nonce = 0
    try:
        nr = requests.get(
            f"{_RELAYER_BASE}/v1/account/transactions/params",
            params={"address": owner_addr, "type": "WALLET"},
            headers=builder_auth_headers("GET", "/v1/account/transactions/params", ""),
            timeout=20,
            proxies=_proxies(),
        )
        if nr.ok:
            nd = nr.json()
            nonce = int(nd.get("nonce") or nd.get("walletNonce") or 0)
    except Exception as e:
        logger.warning("[BUILDER] nonce fetch: %s — using 0", e)

    # authorizeSessionSigner(address) selector
    # keccak256("authorizeSessionSigner(address)")[:4] = 0x... compute via web3
    try:
        from eth_utils import function_signature_to_4byte_selector
        sel = function_signature_to_4byte_selector("authorizeSessionSigner(address)")
    except Exception:
        # fallback common selector if eth_utils path differs
        from eth_utils import keccak
        sel = keccak(text="authorizeSessionSigner(address)")[:4]
    from eth_abi import encode as abi_encode
    calldata = "0x" + sel.hex() + abi_encode(["address"], [session]).hex()

    batch_typed = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Call": [
                {"name": "target", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
            ],
            "Batch": [
                {"name": "wallet", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
                {"name": "calls", "type": "Call[]"},
            ],
        },
        "primaryType": "Batch",
        "domain": {
            "name": "DepositWallet",
            "version": "1",
            "chainId": CHAIN_ID,
            "verifyingContract": deposit,
        },
        "message": {
            "wallet": deposit,
            "nonce": nonce,
            "deadline": deadline,
            "calls": [
                {
                    "target": deposit,
                    "value": 0,
                    "data": bytes.fromhex(calldata[2:]),
                }
            ],
        },
    }
    try:
        signable = encode_typed_data(full_message=batch_typed)
        signed = Account.sign_message(signable, private_key=pk)
        owner_sig = signed.signature.hex()
        if not owner_sig.startswith("0x"):
            owner_sig = "0x" + owner_sig
    except Exception as e:
        logger.exception("[BUILDER] owner batch sign failed")
        return {"success": False, "error": f"Owner sign failed: {e}"}

    path = "/v1/session-signers/authorizations"
    payload = {
        "walletAddress": deposit,
        "sessionSignerAddress": session,
        "scopes": scopes,
        "validUntil": str(valid_until),
        "nonce": str(nonce),
        "deadline": str(deadline),
        "signature": owner_sig,
    }
    body = json.dumps(payload, separators=(",", ":"))
    try:
        headers = builder_auth_headers("POST", path, body)
        headers["Idempotency-Key"] = f"sess-own-{session[:14]}-{valid_until}"
        resp = requests.post(
            f"{_RELAYER_BASE}{path}",
            headers=headers,
            data=body,
            timeout=45,
            proxies=_proxies(),
        )
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text[:500]}
        if resp.ok:
            logger.info(
                "[BUILDER] session authorized (owner-signed) wallet=%s session=%s",
                deposit, session,
            )
            return {
                "success": True,
                "data": data,
                "valid_until": valid_until,
                "scopes": scopes,
                "method": "owner_signed",
            }
        err = data.get("error") or data.get("message") or resp.text[:400]
        logger.warning("[BUILDER] owner-signed authorize failed %s: %s", resp.status_code, err)
        return {"success": False, "error": f"HTTP {resp.status_code}: {err}", "status_code": resp.status_code}
    except Exception as e:
        logger.exception("[BUILDER] authorize_session_key_with_owner")
        return {"success": False, "error": str(e)}


def _get_l2_creds() -> Optional[tuple]:
    """
    Return (key, secret, passphrase) for the *current* signer only.
    User override (session key context): never use operator POLYMARKET_API_* env.
    """
    ov = _override()
    if ov.get("api_key") and ov.get("api_secret") and ov.get("api_passphrase"):
        return ov["api_key"], ov["api_secret"], ov["api_passphrase"]

    signer = (get_signer_address() or "").lower()
    if signer:
        with _l2_cache_lock:
            entry = _l2_cache.get(signer) or {}
        if entry.get("key") and entry.get("secret") and entry.get("passphrase"):
            return entry["key"], entry["secret"], entry["passphrase"]

    # User session context must not pick up operator env L2 keys
    if ov.get("private_key"):
        return None

    key = os.environ.get("POLYMARKET_API_KEY", "").strip()
    secret = os.environ.get("POLYMARKET_API_SECRET", "").strip()
    passphrase = os.environ.get("POLYMARKET_API_PASSPHRASE", "").strip()
    if key and secret and passphrase:
        return key, secret, passphrase
    return None


def _sign_clob_auth(timestamp: str, nonce: int = 0) -> str:
    """EIP-712 ClobAuth signature for L1 credential derivation."""
    from eth_account import Account
    from eth_account.messages import encode_typed_data

    pk = get_private_key()
    if not pk:
        raise ValueError("POLYMARKET_PRIVATE_KEY not set")
    acct = Account.from_key(pk)
    signer = get_signer_address()
    domain = {"name": "ClobAuthDomain", "version": "1", "chainId": CHAIN_ID}
    message = {
        "address": signer,
        "timestamp": timestamp,
        "nonce": nonce,
        "message": _CLOB_AUTH_MESSAGE,
    }
    try:
        signed = acct.sign_typed_data(
            domain_data=domain,
            message_types={"ClobAuth": _CLOB_AUTH_TYPES},
            message_data=message,
        )
    except AttributeError:
        typed = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                "ClobAuth": _CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": domain,
            "message": message,
        }
        signed = acct.sign_message(encode_typed_data(full_message=typed))
    sig = signed.signature.hex()
    return sig if sig.startswith("0x") else "0x" + sig


def _build_l1_headers(nonce: int = 0) -> dict:
    signer = get_signer_address()
    ts = str(int(time.time()))
    signature = _sign_clob_auth(ts, nonce)
    return {
        "POLY_ADDRESS": signer,
        "POLY_SIGNATURE": signature,
        "POLY_TIMESTAMP": ts,
        "POLY_NONCE": str(nonce),
        "Content-Type": "application/json",
    }


def derive_api_key(nonce: int = 0) -> dict:
    """
    Auto-obtain L2 API credentials from private key only.
    1) GET /auth/derive-api-key (existing)
    2) POST /auth/api-key (create new)
    """
    if not get_private_key():
        return {"success": False, "error": "POLYMARKET_PRIVATE_KEY not set"}

    def _extract(data: dict):
        if not isinstance(data, dict):
            return None
        api_key = data.get("apiKey") or data.get("api_key") or data.get("key")
        api_secret = data.get("secret") or data.get("api_secret")
        api_passphrase = data.get("passphrase") or data.get("api_passphrase")
        if api_key and api_secret and api_passphrase:
            return api_key, api_secret, api_passphrase
        return None

    try:
        headers = _build_l1_headers(nonce)
        resp = requests.get(
            f"{CLOB_BASE}/auth/derive-api-key",
            headers=headers,
            timeout=15,
            proxies=_proxies(),
        )
        logger.info("[POLY:AUTH] GET /auth/derive-api-key → %s", resp.status_code)
        if resp.ok:
            found = _extract(resp.json())
            if found:
                logger.info("[POLY:AUTH] Derived existing L2 credentials")
                return {
                    "success": True,
                    "api_key": found[0],
                    "api_secret": found[1],
                    "api_passphrase": found[2],
                }
    except Exception as e:
        logger.info("[POLY:AUTH] derive-api-key attempt: %s", e)

    try:
        headers = _build_l1_headers(nonce)
        resp = requests.post(
            f"{CLOB_BASE}/auth/api-key",
            headers=headers,
            timeout=15,
            proxies=_proxies(),
        )
        logger.info("[POLY:AUTH] POST /auth/api-key → %s", resp.status_code)
        if not resp.ok:
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:400]}"}
        found = _extract(resp.json())
        if not found:
            return {"success": False, "error": f"Unexpected response: {resp.text[:300]}"}
        logger.info("[POLY:AUTH] Created new L2 API credentials (cached in-process)")
        return {
            "success": True,
            "api_key": found[0],
            "api_secret": found[1],
            "api_passphrase": found[2],
        }
    except Exception as e:
        logger.error("[POLY:AUTH] derive_api_key failed: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}


def ensure_l2_creds(force_derive: bool = False) -> dict:
    """
    Resolve L2 API credentials for the *current* signer (operator or session key).

    Polymarket requires: order.signer == address that owns the API key.
    Multi-user: when push_user_creds is active, L2 keys are ALWAYS derived from
    that user's session private key (never operator POLYMARKET_API_* from .env).
    Cache is keyed by signer address so concurrent users do not cross-contaminate.
    """
    ov = _override()
    use_env = os.environ.get("POLYMARKET_USE_ENV_API_KEYS", "").strip().lower() in (
        "1", "true", "yes",
    )
    # User context: never use operator env API keys
    if ov.get("private_key"):
        use_env = False

    signer = (get_signer_address() or "").lower()
    if force_derive and signer:
        with _l2_cache_lock:
            _l2_cache.pop(signer, None)

    # User-supplied L2 keys on the override (must match session signer)
    if ov.get("api_key") and ov.get("api_secret") and ov.get("api_passphrase"):
        return {
            "success": True,
            "api_key": ov["api_key"],
            "api_secret": ov["api_secret"],
            "api_passphrase": ov["api_passphrase"],
            "source": "user_override",
            "signer": signer,
        }

    # Cache hit for this signer
    if signer and not force_derive:
        with _l2_cache_lock:
            entry = _l2_cache.get(signer) or {}
        if entry.get("key") and entry.get("secret") and entry.get("passphrase"):
            return {
                "success": True,
                "api_key": entry["key"],
                "api_secret": entry["secret"],
                "api_passphrase": entry["passphrase"],
                "source": "cache",
                "signer": signer,
            }

    # Derive from current private key (session key under override, else operator)
    if get_private_key() and (force_derive or not use_env or ov.get("private_key")):
        result = derive_api_key()
        if result.get("success"):
            if signer:
                with _l2_cache_lock:
                    _l2_cache[signer] = {
                        "key": result["api_key"],
                        "secret": result["api_secret"],
                        "passphrase": result["api_passphrase"],
                    }
            result["source"] = "derived"
            result["signer"] = signer
            return result
        # User context must not fall through to operator env keys
        if ov.get("private_key"):
            return {
                "success": False,
                "error": (
                    f"Could not derive L2 API key for session signer {signer}: "
                    f"{result.get('error')}. Authorize the session key on Polymarket, "
                    "then retry."
                ),
                "signer": signer,
            }

    existing = _get_l2_creds()
    if existing:
        return {
            "success": True,
            "api_key": existing[0],
            "api_secret": existing[1],
            "api_passphrase": existing[2],
            "source": "cached_or_env",
            "signer": signer,
        }

    result = derive_api_key()
    if result.get("success"):
        if signer:
            with _l2_cache_lock:
                _l2_cache[signer] = {
                    "key": result["api_key"],
                    "secret": result["api_secret"],
                    "passphrase": result["api_passphrase"],
                }
        result["source"] = "derived"
        result["signer"] = signer
        return result
    return {
        "success": False,
        "error": result.get("error") or "L2 credential derivation failed",
        "signer": signer,
    }



def get_relayer_api_key() -> Optional[str]:
    return (
        os.environ.get("POLYMARKET_RELAYER_API_KEY", "").strip()
        or os.environ.get("RELAYER_API_KEY", "").strip()
        or None
    )


def get_relayer_api_key_address() -> Optional[str]:
    return (
        os.environ.get("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()
        or os.environ.get("RELAYER_API_KEY_ADDRESS", "").strip()
        or None
    )


def _relayer_headers(method: str, path: str, body: str = "") -> dict:
    """Prefer Builder HMAC auth; fall back to Relayer API key headers."""
    if builder_configured():
        return builder_auth_headers(method, path, body)
    key = get_relayer_api_key()
    addr = get_relayer_api_key_address()
    if key and addr:
        return {
            "Content-Type": "application/json",
            "RELAYER_API_KEY": key,
            "RELAYER_API_KEY_ADDRESS": addr,
        }
    return {"Content-Type": "application/json"}


def _encode_erc20_approve(spender: str, amount: int = (2**256 - 1)) -> str:
    # approve(address,uint256) selector 0x095ea7b3
    spender_clean = spender.lower().replace("0x", "").zfill(64)
    amt_hex = hex(amount)[2:].zfill(64)
    return "0x095ea7b3" + spender_clean + amt_hex


def _encode_erc1155_set_approval_for_all(operator: str, approved: bool = True) -> str:
    # setApprovalForAll(address,bool) selector 0xa22cb465
    op = operator.lower().replace("0x", "").zfill(64)
    flag = "1".zfill(64) if approved else "0".zfill(64)
    return "0xa22cb465" + op + flag


def ensure_trading_approvals(
    *,
    wallet: Optional[str] = None,
    private_key: Optional[str] = None,
    force: bool = False,
) -> dict:
    """
    Submit gasless token approvals via Polymarket Relayer so the wallet can trade.

    Approves:
      - pUSD → CTF (split)
      - CTF outcome tokens → CTF Exchange + Neg Risk Exchange (trade)

    Requires Builder API keys and/or Relayer API key in .env.
    User/operator only needs pUSD — gas is paid by Polymarket.
    """
    wallet = (wallet or get_deposit_wallet_address() or get_funder_address() or "").strip()
    if not wallet:
        return {"success": False, "error": "No wallet/funder for approvals"}
    key = (private_key or get_private_key() or "").strip()
    if not key:
        return {"success": False, "error": "No private key / session key to sign approvals"}

    wkey = wallet.lower()
    if not force and wkey in _approvals_done:
        return {"success": True, "skipped": True, "reason": "already_submitted_this_process"}

    if not builder_configured() and not (get_relayer_api_key() and get_relayer_api_key_address()):
        return {
            "success": False,
            "error": "Set POLYMARKET_BUILDER_* or POLYMARKET_RELAYER_API_KEY(+_ADDRESS) for gasless approvals",
        }

    pusd = PUSD_ADDRESS
    txs = [
        {"to": pusd, "data": _encode_erc20_approve(_CTF), "value": "0"},
        {"to": _CTF, "data": _encode_erc1155_set_approval_for_all(_EXCHANGE), "value": "0"},
        {"to": _CTF, "data": _encode_erc1155_set_approval_for_all(_NEG_RISK_EXCHANGE), "value": "0"},
        {"to": _CTF, "data": _encode_erc1155_set_approval_for_all(_NEG_RISK_ADAPTER), "value": "0"},
    ]

    # Relayer expects a batch under PROXY/SAFE/WALLET styles depending on wallet type.
    # Use generic PROXY-TX style body commonly accepted with builder auth.
    body_obj = {
        "type": "SAFE",
        "from": wallet,
        "to": wallet,
        "proxyWallet": wallet,
        "data": "0x",
        "transactions": txs,
        "metadata": "SignalDesk trading approvals (pUSD+CTF)",
    }
    # Simpler alternate: some deployments use execute batch
    body_alt = {
        "type": "PROXY",
        "from": wallet,
        "to": wallet,
        "data": "0x",
        "transactions": txs,
        "metadata": "SignalDesk trading approvals",
    }

    path = "/submit"
    results = []
    for body_obj_try in (body_obj, body_alt):
        try:
            body = json.dumps(body_obj_try, separators=(",", ":"))
            headers = _relayer_headers("POST", path, body)
            headers["Content-Type"] = "application/json"
            resp = requests.post(
                f"https://relayer-v2.polymarket.com{path}",
                headers=headers,
                data=body,
                timeout=30,
                proxies=_proxies(),
            )
            text = (resp.text or "")[:500]
            results.append({"status": resp.status_code, "body": text, "type": body_obj_try.get("type")})
            if resp.ok:
                _approvals_done.add(wkey)
                logger.info("[RELAYER] approvals submitted wallet=%s type=%s", wallet, body_obj_try.get("type"))
                return {"success": True, "response": resp.json() if resp.text else {}, "attempt": body_obj_try.get("type")}
        except Exception as e:
            results.append({"error": str(e), "type": body_obj_try.get("type")})
            logger.warning("[RELAYER] approvals attempt failed: %s", e)

    return {
        "success": False,
        "error": "Relayer approval submit failed — check Builder/Relayer keys and wallet type",
        "attempts": results,
        "note": "Approvals may already exist on-chain; order placement can still work.",
    }



def credentials_status() -> dict:
    """Safe for Settings UI — never raise; report env presence first."""
    err = None
    try:
        pk_raw = get_private_key()
        pk = bool(pk_raw)
    except Exception as e:
        pk = False
        err = f"private_key: {e}"
    try:
        funder = get_funder_address()
    except Exception as e:
        funder = None
        err = (err or "") + f" funder: {e}"
    try:
        signer = get_signer_address()
    except Exception:
        signer = None
    try:
        deposit = get_deposit_wallet_address()
    except Exception:
        deposit = funder
    try:
        sig_t = get_signature_type()
    except Exception:
        sig_t = 1
    try:
        l2 = _get_l2_creds()
    except Exception:
        l2 = None
    try:
        relayer_key = bool(get_relayer_api_key())
    except Exception:
        relayer_key = False
    try:
        relayer_addr = get_relayer_api_key_address()
    except Exception:
        relayer_addr = None
    try:
        bcfg = builder_configured()
        bkey = bool(get_builder_api_key())
    except Exception:
        bcfg, bkey = False, False
    return {
        "private_key_set": pk,
        "signer": signer,
        "funder": funder,
        "deposit_wallet": deposit,
        "signature_type": sig_t,
        "signature_type_name": {0: "EOA", 1: "POLY_PROXY", 2: "GNOSIS_SAFE", 3: "POLY_1271"}.get(sig_t, str(sig_t)),
        "l2_ready": bool(l2),
        "relayer_api_key_set": relayer_key,
        "builder_configured": bcfg,
        "builder_api_key_set": bkey,
        "builder_code_set": bool(get_builder_code()),
        "builder_code": (lambda c: (c[:10] + "…" + c[-6:]) if c else None)(get_builder_code()),
        "relayer_api_key_address": relayer_addr,
        "live_ready": bool(pk and funder),
        "error": err,
        "note": (
            "Operator live: POLYMARKET_PRIVATE_KEY + POLYMARKET_FUNDER_ADDRESS + "
            "POLYMARKET_SIGNATURE_TYPE (1=proxy, 2=Safe, 3=deposit). "
            "L2 CLOB keys derive from the private key."
        ),
    }





def get_user_trades(
    user: Optional[str] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    *,
    side: Optional[str] = None,
    result: Optional[str] = None,
    pair: Optional[str] = None,
    limit: int = 500,
) -> dict:
    """
    Fetch CLOB/Data-API trades for the funder in [start_ts, end_ts].
    Resolves PnL via official Polymarket market outcomes only.
    Filters: side BUY|SELL, result win|loss|open, pair BTC|ETH.
    """
    funder = (user or get_funder_address() or "").strip()
    if not funder:
        return {
            "success": False,
            "error": "No funder address",
            "trades": [],
            "count": 0,
            "volume_usd": 0.0,
            "realized_pnl": 0.0,
            "resolved_count": 0,
            "open_count": 0,
        }

    now = datetime.now(timezone.utc)
    if end_ts is None:
        end_ts = int(now.timestamp()) + 60
    if start_ts is None:
        # default: last 7 days for history pages; callers pass today for dashboard
        start_ts = int((now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)).timestamp())

    trades: list = []
    offset = 0
    page = 100
    try:
        while offset < max(limit, 500):
            r = requests.get(
                "https://data-api.polymarket.com/trades",
                params={
                    "user": funder,
                    "limit": page,
                    "offset": offset,
                    "takerOnly": "false",
                    "start": int(start_ts),
                    "end": int(end_ts),
                },
                timeout=15,
            )
            if r.status_code != 200:
                return {
                    "success": False,
                    "error": f"Data API HTTP {r.status_code}: {r.text[:200]}",
                    "trades": trades,
                    "count": len(trades),
                    "volume_usd": 0.0,
                    "realized_pnl": 0.0,
                    "user": funder,
                }
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            trades.extend(batch)
            if len(batch) < page:
                break
            offset += page
            if len(trades) >= limit * 2:
                break
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "trades": trades,
            "count": len(trades),
            "volume_usd": 0.0,
            "realized_pnl": 0.0,
            "user": funder,
        }

    filtered = []
    for tr in trades:
        ts = tr.get("timestamp") or tr.get("match_time") or 0
        try:
            ts = int(ts)
        except (TypeError, ValueError):
            continue
        if ts < int(start_ts) or ts > int(end_ts):
            continue
        filtered.append(tr)

    res_cache: dict = {}

    def _winner_for(tr: dict) -> Optional[str]:
        slug = tr.get("slug") or tr.get("eventSlug")
        cid = tr.get("conditionId") or tr.get("condition_id")
        key = cid or slug or ""
        if not key:
            return None
        if key in res_cache:
            return res_cache[key]
        w = None
        try:
            info = get_market_resolution(slug=slug, condition_id=cid)
            if info.get("resolved"):
                w = info.get("winning_side")
        except Exception:
            w = None
        res_cache[key] = w
        return w

    def _pair_label(tr: dict) -> str:
        blob = " ".join(str(x or "") for x in (tr.get("title"), tr.get("slug"), tr.get("eventSlug"))).lower()
        if "bitcoin" in blob or "btc" in blob:
            return "BTC"
        if "ethereum" in blob or "eth" in blob:
            return "ETH"
        return ""

    volume = 0.0
    realized = 0.0
    resolved_n = 0
    open_n = 0
    compact = []
    side_f = (side or "").upper().strip() or None
    result_f = (result or "").lower().strip() or None
    pair_f = (pair or "").upper().replace("USDT", "").strip() or None

    for tr in filtered:
        try:
            size = float(tr.get("size") or 0)
            price = float(tr.get("price") or 0)
            notional = round(size * price, 4)
        except (TypeError, ValueError):
            size, price, notional = 0.0, 0.0, 0.0

        outcome = (tr.get("outcome") or "").strip()
        side_v = (tr.get("side") or "").upper()
        pair_v = _pair_label(tr)

        if side_f and side_v != side_f:
            continue
        if pair_f and pair_v != pair_f:
            continue

        winner = _winner_for(tr)
        pnl = None
        status = "open"
        if winner:
            status = "resolved"
            oc = outcome.lower()
            win_side = winner.upper()
            bought_up = oc in ("up", "yes")
            bought_down = oc in ("down", "no")
            won = (bought_up and win_side == "UP") or (bought_down and win_side == "DOWN")
            if side_v == "SELL":
                won = not won
            if won:
                pnl = round(size * (1.0 - price), 4)
            else:
                pnl = round(-size * price, 4)
        else:
            open_n += 1

        row_result = None
        if pnl is not None:
            row_result = "win" if pnl > 0 else ("loss" if pnl < 0 else "flat")
            resolved_n += 1
            realized += pnl
            volume += notional
        else:
            volume += notional

        if result_f == "open" and status != "open":
            continue
        if result_f in ("win", "loss") and row_result != result_f:
            continue

        ts_raw = tr.get("timestamp")
        try:
            ts_i = int(ts_raw)
            time_utc = datetime.fromtimestamp(ts_i, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            time_short = datetime.fromtimestamp(ts_i, tz=timezone.utc).strftime("%H:%M:%S")
        except Exception:
            ts_i, time_utc, time_short = ts_raw, "—", "—"

        compact.append({
            "side": side_v,
            "outcome": outcome,
            "pair": pair_v or "—",
            "title": (tr.get("title") or tr.get("slug") or "")[:100],
            "slug": tr.get("slug"),
            "size": size,
            "price": price,
            "notional": notional,
            "timestamp": ts_i,
            "time_utc": time_utc,
            "time_short": time_short,
            "tx": tr.get("transactionHash") or tr.get("transaction_hash"),
            "status": status,
            "winner": winner,
            "pnl": pnl,
            "result": row_result,
        })

    compact.sort(key=lambda x: x.get("timestamp") or 0, reverse=True)
    compact = compact[:limit]

    return {
        "success": True,
        "user": funder,
        "trades": compact,
        "count": len(compact),
        "volume_usd": round(volume, 2),
        "realized_pnl": round(realized, 4),
        "resolved_count": resolved_n,
        "open_count": open_n,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
    }


def get_user_trades_today(user: Optional[str] = None, day_start_ts: Optional[int] = None) -> dict:
    now = datetime.now(timezone.utc)
    if day_start_ts is None:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = int(day_start.timestamp())
    return get_user_trades(
        user=user,
        start_ts=int(day_start_ts),
        end_ts=int(now.timestamp()) + 60,
        limit=200,
    )



def get_onchain_balance(funder: Optional[str] = None, signer: Optional[str] = None) -> dict:
    """pUSD balance of funder (+ optional POL of signer). Pass funder= for per-user wallets."""
    funder = (funder or get_funder_address() or "").strip() or None
    signer = (signer or get_signer_address() or "").strip() or None
    if not funder:
        return {"success": False, "error": "Funder address not configured"}
    try:
        from web3 import Web3
        # Prefer POLYMER_RPC_URL / POLYGON_RPC_URL, then known-working public endpoints
        rpcs = [
            os.environ.get("POLYGON_RPC_URL", "").strip(),
            os.environ.get("POLYMER_RPC_URL", "").strip(),
            "https://rpc-mainnet.matic.quiknode.pro",
            "https://polygon.gateway.tenderly.co",
            "https://polygon-bor-rpc.publicnode.com",
            "https://polygon-rpc.com",
        ]
        w3 = None
        for rpc in rpcs:
            if not rpc:
                continue
            try:
                c = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 8}))
                if c.is_connected():
                    w3 = c
                    break
            except Exception:
                continue
        if not w3:
            return {"success": False, "error": "No Polygon RPC reachable"}

        abi = [{
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function",
        }]
        pusd = w3.eth.contract(address=Web3.to_checksum_address(PUSD_ADDRESS), abi=abi)
        raw = pusd.functions.balanceOf(funder).call()
        pusd_bal = raw / 1_000_000
        pol_bal = None
        if signer:
            pol_bal = float(w3.from_wei(w3.eth.get_balance(signer), "ether"))
        return {
            "success": True,
            "funder": funder,
            "signer": signer,
            "pusd_balance": round(pusd_bal, 4),
            "pol_balance": round(pol_bal, 5) if pol_bal is not None else None,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── Market discovery ─────────────────────────────────────────────────────────


# ── Market discovery ─────────────────────────────────────────────────────────
# 15m: btc-updown-15m-{unix_window_start}
# 1h:  bitcoin-up-or-down-{month}-{day}-{year}-{h}{am|pm}-et  (America/New_York)

_ASSET_HOURLY_NAME = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "xrp",
    "bnb": "bnb",
    "doge": "dogecoin",
}


def _window_start(ts: Optional[int], timeframe: str) -> int:
    if ts is None:
        ts = int(time.time())
    ts = int(ts)
    if timeframe == "1h":
        return ts - (ts % 3600)
    if timeframe == "5m":
        return ts - (ts % 300)
    return ts - (ts % 900)


def _hourly_et_slug(asset_short: str, window_ts: int) -> str:
    """
    Build Polymarket 1h up/down event slug for the ET hour containing window_ts.
    Example: bitcoin-up-or-down-august-19-2026-9am-et
    Verified live against gamma-api 2026-08-19.
    """
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        et = timezone.utc  # fallback; may miss slug
    # window_ts is entry open (UTC unix). Convert that instant to ET wall hour.
    dt_utc = datetime.fromtimestamp(int(window_ts), tz=timezone.utc)
    dt_et = dt_utc.astimezone(et) if et != timezone.utc else dt_utc
    # Floor to hour in ET
    dt_et = dt_et.replace(minute=0, second=0, microsecond=0)
    month = dt_et.strftime("%B").lower()
    day = str(dt_et.day)
    year = str(dt_et.year)
    h = dt_et.hour
    if h == 0:
        h12, ampm = 12, "am"
    elif h < 12:
        h12, ampm = h, "am"
    elif h == 12:
        h12, ampm = 12, "pm"
    else:
        h12, ampm = h - 12, "pm"
    name = _ASSET_HOURLY_NAME.get(asset_short, asset_short)
    return f"{name}-up-or-down-{month}-{day}-{year}-{h12}{ampm}-et"


def _parse_market_payload(market: dict, slug: str) -> dict:
    tokens = market.get("clobTokenIds") or market.get("tokens") or []
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = []
    outcomes = market.get("outcomes") or ["Up", "Down"]
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = ["Up", "Down"]

    up_token = down_token = None
    if isinstance(tokens, list) and len(tokens) >= 2:
        for i, o in enumerate(outcomes):
            o_l = str(o).lower()
            tid = tokens[i] if i < len(tokens) else None
            if isinstance(tid, dict):
                tid = tid.get("token_id") or tid.get("tokenId")
            if o_l in ("up", "yes"):
                up_token = str(tid)
            elif o_l in ("down", "no"):
                down_token = str(tid)
        if up_token is None and len(tokens) >= 2:
            t0, t1 = tokens[0], tokens[1]
            if isinstance(t0, dict):
                t0 = t0.get("token_id") or t0.get("tokenId")
            if isinstance(t1, dict):
                t1 = t1.get("token_id") or t1.get("tokenId")
            up_token, down_token = str(t0), str(t1)

    return {
        "slug": market.get("slug") or slug,
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "up_token_id": up_token,
        "down_token_id": down_token,
        "tick_size": market.get("minimum_tick_size") or market.get("orderPriceMinTickSize") or market.get("tickSize") or "0.01",
        "neg_risk": bool(market.get("negRisk") or market.get("neg_risk")),
        "closed": bool(market.get("closed")),
        "raw": market,
    }


def discover_market(symbol: str, timeframe: str = "15m", anchor_ts: Optional[int] = None) -> Optional[dict]:
    """
    Discover Polymarket crypto up/down market for the signal window.

    15m slug: {btc|eth|...}-updown-15m-{window_start_unix}
    1h  slug: {bitcoin|ethereum|...}-up-or-down-{month}-{d}-{yyyy}-{h}{am|pm}-et
              (queried via /events?slug= then markets[0])
    """
    asset = symbol.replace("USDT", "").replace("USD", "").lower()
    window = _window_start(anchor_ts, timeframe)

    try:
        if timeframe == "1h":
            slug = _hourly_et_slug(asset, window)
            # Primary: events API (1h markets are event-wrapped)
            resp = requests.get(
                f"{GAMMA_BASE}/events",
                params={"slug": slug},
                timeout=10,
                proxies=_proxies(),
            )
            market = None
            if resp.ok:
                data = resp.json()
                events = data if isinstance(data, list) else ([data] if isinstance(data, dict) and data.get("slug") else [])
                if events:
                    ev = events[0]
                    markets = ev.get("markets") or []
                    if markets:
                        market = markets[0]
                        # prefer event slug for resolution lookups
                        if not market.get("slug"):
                            market["slug"] = ev.get("slug") or slug
            if not market:
                # Fallback: markets?slug=
                resp2 = requests.get(
                    f"{GAMMA_BASE}/markets",
                    params={"slug": slug},
                    timeout=10,
                    proxies=_proxies(),
                )
                if resp2.ok:
                    data2 = resp2.json()
                    markets = data2 if isinstance(data2, list) else data2.get("data") or []
                    if markets:
                        market = markets[0]
            if not market:
                logger.warning("[POLY] 1h market not found slug=%s (asset=%s window=%s)", slug, asset, window)
                return None
            parsed = _parse_market_payload(market, slug)
            logger.info("[POLY] 1h discovered slug=%s tokens_ok=%s", slug, bool(parsed.get("up_token_id") and parsed.get("down_token_id")))
            return parsed

        if timeframe == "5m":
            slug = f"{asset}-updown-5m-{window}"
            resp = requests.get(f"{GAMMA_BASE}/markets", params={"slug": slug}, timeout=6, proxies=_proxies())
            markets = []
            if resp.ok:
                data = resp.json()
                markets = data if isinstance(data, list) else data.get("data") or []
            if not markets:
                resp_e = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=6, proxies=_proxies())
                if resp_e.ok:
                    data_e = resp_e.json()
                    events = data_e if isinstance(data_e, list) else ([data_e] if isinstance(data_e, dict) and data_e.get("markets") else [])
                    if events:
                        markets = events[0].get("markets") or []
            if not markets:
                logger.warning("[POLY] 5m market not found slug=%s", slug)
                return None
            return _parse_market_payload(markets[0], slug)
        # 15m path
        slug = f"{asset}-updown-15m-{window}"
        resp = requests.get(
            f"{GAMMA_BASE}/markets",
            params={"slug": slug},
            timeout=10,
            proxies=_proxies(),
        )
        markets = []
        if resp.ok:
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("data") or []
        if not markets:
            # also try events
            resp_e = requests.get(
                f"{GAMMA_BASE}/events",
                params={"slug": slug},
                timeout=10,
                proxies=_proxies(),
            )
            if resp_e.ok:
                data_e = resp_e.json()
                events = data_e if isinstance(data_e, list) else ([data_e] if isinstance(data_e, dict) and data_e.get("markets") else [])
                if events:
                    markets = events[0].get("markets") or []
        if not markets:
            logger.warning("[POLY] 15m market not found slug=%s", slug)
            return None
        return _parse_market_payload(markets[0], slug)
    except Exception as e:
        logger.error("[POLY] discover_market error: %s", e)
        return None




def get_market_resolution(slug: Optional[str] = None, condition_id: Optional[str] = None) -> dict:
    """
    Official Polymarket resolution (Gamma + CLOB only).

    Crypto 15m up/down: Polymarket UI shows the winner at the boundary (:00/:15/:30/:45)
    via outcomePrices ~0/1, then opens the next market. We accept price >= 0.99 or
    closed/UMA as resolved. Callers MUST only call this after candle_close_time so
    pre-close odds cannot book a result early.
    """
    empty = {"resolved": False, "winning_side": None, "source": None}

    def _as_list(v):
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return []
        return []

    def _winner_from_market(market: dict, source: str) -> dict:
        if not market:
            return {**empty, "error": "empty market"}

        outcomes = _as_list(market.get("outcomes"))
        prices = _as_list(market.get("outcomePrices") or market.get("outcome_prices"))
        tokens = market.get("tokens") or []
        if isinstance(tokens, str):
            tokens = _as_list(tokens)

        uma = str(market.get("umaResolutionStatus") or market.get("uma_resolution_status") or "").lower()
        closed = bool(market.get("closed", False))

        # CLOB tokens: winner flag or extreme price
        if tokens and isinstance(tokens, list) and tokens and isinstance(tokens[0], dict):
            for tok in tokens:
                wflag = tok.get("winner")
                if wflag is True or str(wflag).lower() == "true":
                    ol = str(tok.get("outcome") or "").lower()
                    winning = "UP" if ol in ("up", "yes") else ("DOWN" if ol in ("down", "no") else None)
                    if winning:
                        return {
                            "resolved": True,
                            "winning_side": winning,
                            "closed": closed,
                            "source": f"{source}:clob_winner",
                            "raw_outcome": tok.get("outcome"),
                        }
            # price on token near 1.0 (UI settles at boundary; time-gated by caller)
            for tok in tokens:
                try:
                    pr = float(tok.get("price") or 0)
                except (TypeError, ValueError):
                    continue
                if pr >= 0.99:
                    ol = str(tok.get("outcome") or "").lower()
                    winning = "UP" if ol in ("up", "yes") else ("DOWN" if ol in ("down", "no") else None)
                    if winning:
                        return {
                            "resolved": True,
                            "winning_side": winning,
                            "closed": closed,
                            "source": f"{source}:clob_price",
                            "raw_outcome": tok.get("outcome"),
                            "price": pr,
                        }

        winning = None
        raw_label = None
        max_p = 0.0
        if outcomes and prices and len(outcomes) == len(prices):
            for o, pr in zip(outcomes, prices):
                try:
                    pf = float(pr)
                except (TypeError, ValueError):
                    continue
                if pf > max_p:
                    max_p = pf
                if pf >= 0.99:
                    raw_label = str(o)
                    ol = raw_label.lower()
                    winning = "UP" if ol in ("up", "yes") else ("DOWN" if ol in ("down", "no") else None)
                    break

        # Polymarket UI flips the winner at the 15m boundary via outcomePrices (~0/1)
        # often before closed=true. Callers MUST only poll after candle_close_time so
        # pre-close odds cannot book a result early.
        formally_settled = closed or uma in ("resolved", "settled", "finalized")
        if winning is not None and formally_settled:
            return {
                "resolved": True,
                "winning_side": winning,
                "closed": closed,
                "uma": uma or None,
                "source": f"{source}:closed_or_uma",
                "raw_outcome": raw_label,
                "price": max_p,
            }
        # UI-speed path: price already at 0.99 (same signal the site shows at :00/:15/:30/:45)
        if winning is not None and max_p >= 0.99:
            return {
                "resolved": True,
                "winning_side": winning,
                "closed": closed,
                "uma": uma or None,
                "source": f"{source}:outcome_prices",
                "raw_outcome": raw_label,
                "price": max_p,
            }
        return {
            "resolved": False,
            "winning_side": None,
            "closed": closed,
            "uma": uma or None,
            "source": source,
            "max_price": max_p,
        }

    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        market = None
        last = {**empty}

        def _fetch_clob():
            if not condition_id:
                return None
            resp = requests.get(
                f"{CLOB_BASE}/markets/{condition_id}",
                timeout=2.5,
                proxies=_proxies(),
            )
            if resp.ok:
                data = resp.json()
                if isinstance(data, dict):
                    return data
            return None

        def _fetch_gamma(params):
            resp = requests.get(
                f"{GAMMA_BASE}/markets",
                params=params,
                timeout=2.5,
                proxies=_proxies(),
            )
            if not resp.ok:
                return None
            data = resp.json()
            markets = data if isinstance(data, list) else data.get("data") or []
            return markets[0] if markets else None

        # Fire CLOB + Gamma in parallel — take first resolved
        jobs = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            if condition_id:
                jobs[pool.submit(_fetch_clob)] = "clob"
                jobs[pool.submit(_fetch_gamma, {"condition_ids": condition_id})] = "gamma_cid"
            if slug:
                jobs[pool.submit(_fetch_gamma, {"slug": slug})] = "gamma_slug"
            for fut in as_completed(jobs):
                src = jobs[fut]
                try:
                    mkt = fut.result()
                except Exception as e:
                    logger.debug("resolve fetch %s: %s", src, e)
                    continue
                if not mkt:
                    continue
                market = mkt
                out = _winner_from_market(mkt, src.split("_")[0])
                if out.get("resolved"):
                    return out
                last = out

        if market:
            return last if last else _winner_from_market(market, "gamma")
        return {**empty, "error": "market not found on Polymarket"}
    except Exception as e:
        return {**empty, "error": str(e)}





# ── Order book / market price (official CLOB behavior) ─────────────────────
# Docs: FAK/FOK BUY amount = USD to spend; price = worst-price limit from book.
# GTC = share size (book min often 5). Ref:
# https://docs.polymarket.com/trading/orders/create
# https://docs.polymarket.com/trading/place-orders


def get_order_book(token_id: str) -> dict:
    """Fetch CLOB book for token. Returns asks/bids sorted best-first."""
    empty = {"asks": [], "bids": [], "tick_size": "0.01", "min_order_size": "5", "error": None}
    if not token_id:
        return {**empty, "error": "missing token_id"}
    try:
        resp = requests.get(
            f"{CLOB_BASE}/book",
            params={"token_id": str(token_id)},
            timeout=12,
            proxies=_proxies(),
        )
        if not resp.ok:
            return {**empty, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
        data = resp.json()
        asks = data.get("asks") or []
        bids = data.get("bids") or []
        # Normalize to list of {price, size} floats; asks ascending (best = lowest)
        def _norm(levels):
            out = []
            for lv in levels:
                if isinstance(lv, dict):
                    pr = float(lv.get("price") or lv.get("p") or 0)
                    sz = float(lv.get("size") or lv.get("s") or 0)
                elif isinstance(lv, (list, tuple)) and len(lv) >= 2:
                    pr, sz = float(lv[0]), float(lv[1])
                else:
                    continue
                if pr > 0 and sz > 0:
                    out.append({"price": pr, "size": sz})
            return out
        asks_n = sorted(_norm(asks), key=lambda x: x["price"])
        bids_n = sorted(_norm(bids), key=lambda x: -x["price"])
        return {
            "asks": asks_n,
            "bids": bids_n,
            "tick_size": str(data.get("tick_size") or "0.01"),
            "min_order_size": str(data.get("min_order_size") or "5"),
            "neg_risk": data.get("neg_risk"),
            "error": None,
            "raw_keys": list(data.keys()) if isinstance(data, dict) else [],
        }
    except Exception as e:
        return {**empty, "error": str(e)}


def estimate_buy_market_price(asks: list, amount_usd: float, order_type: str = "FAK") -> dict:
    """
    Walk asks (best first) until USD notional is covered.
    Mirrors py-clob-client-v2 OrderBuilder.calculate_buy_market_price.
    Returns worst price level needed, or error if book empty / insufficient for FOK.
    """
    if not asks:
        return {"success": False, "error": "no asks on book", "price": None}
    remaining = float(amount_usd)
    worst = None
    filled_usd = 0.0
    for lvl in asks:  # ascending price
        pr = float(lvl["price"])
        sz = float(lvl["size"])
        level_usd = pr * sz
        worst = pr
        if level_usd >= remaining:
            filled_usd += remaining
            remaining = 0.0
            break
        filled_usd += level_usd
        remaining -= level_usd
    if remaining > 1e-9 and order_type.upper() == "FOK":
        return {
            "success": False,
            "error": f"insufficient liquidity for FOK (need ${amount_usd}, depth ~${filled_usd:.2f})",
            "price": worst,
        }
    # FAK: use deepest price reached (or best ask if nothing walked)
    if worst is None:
        worst = float(asks[0]["price"])
    return {
        "success": True,
        "price": worst,
        "best_ask": float(asks[0]["price"]),
        "filled_usd_estimate": round(filled_usd, 4),
    }


# ── Shadow order ─────────────────────────────────────────────────────────────

def place_shadow_order(
    symbol: str, direction: str, position_size_usd: float,
    timeframe: str = "15m", order_type: str = "FAK",
    max_contract_price: float = 0.80, candle_open_ts: Optional[int] = None,
) -> dict:
    """Fill only if live best_ask on THIS TF market <= max_contract_price. Never invent fill."""
    tf = (timeframe or "15m").lower()
    if candle_open_ts is not None:
        wsec = 300 if tf == "5m" else (3600 if tf == "1h" else 900)
        candle_open_ts = int(candle_open_ts) - (int(candle_open_ts) % wsec)
    market = discover_market(symbol, timeframe=tf, anchor_ts=candle_open_ts)
    user_cap = min(float(max_contract_price if max_contract_price is not None else 0.99), 0.99)
    if not market:
        err = f"no market tf={tf} window={candle_open_ts}"
        logger.info("[POLY:SHADOW] skip %s %s — %s", symbol, direction, err)
        return {"success": False, "error": err, "shadow": True,
                "debug": {"timeframe": tf, "user_cap": user_cap}}
    token_id = market.get("up_token_id") if direction.upper() == "UP" else market.get("down_token_id")
    if not token_id:
        return {"success": False, "error": "no token_id", "shadow": True}
    best_ask = book_err = None
    try:
        book = get_order_book(str(token_id))
        book_err = book.get("error")
        asks = book.get("asks") or []
        if asks:
            best_ask = float(asks[0]["price"])
    except Exception as e:
        book_err = str(e)
    logger.info("[POLY:SHADOW] book %s %s tf=%s slug=%s ask=%s cap=%.4f",
                symbol, direction, tf, market.get("slug"),
                f"{best_ask:.4f}" if best_ask is not None else "None", user_cap)
    if best_ask is None:
        err = f"no discount liquidity: empty book cap={user_cap:.4f}" + (f" err={book_err}" if book_err else "")
        return {"success": False, "error": err, "shadow": True,
                "debug": {"best_ask": None, "user_cap": user_cap, "timeframe": tf, "slug": market.get("slug")}}
    if best_ask > user_cap + 1e-9:
        err = f"no discount liquidity: best_ask={best_ask:.4f} > max={user_cap:.4f}"
        return {"success": False, "error": err, "shadow": True,
                "debug": {"best_ask": best_ask, "user_cap": user_cap, "timeframe": tf, "slug": market.get("slug")}}
    price = float(best_ask)
    size = round(position_size_usd / price, 4) if price > 0 else 0.0
    logger.info("[POLY:SHADOW] FILL %s %s tf=%s ask=%.4f ≤ cap=%.4f slug=%s",
                symbol, direction, tf, price, user_cap, market.get("slug"))
    return {
        "success": True, "shadow": True,
        "order_id": f"shadow_{int(time.time())}_{random.randint(1000,9999)}",
        "contracts": size, "price_per_contract": price, "price": price,
        "total_spent": round(price * size, 4),
        "slug": market.get("slug"), "condition_id": market.get("condition_id"),
        "token_id": token_id, "signal_direction": direction.upper(),
        "order_type": order_type, "platform": "polymarket",
        "debug": {"best_ask": best_ask, "user_cap": user_cap, "timeframe": tf},
    }


def place_live_order(
    symbol: str,
    direction: str,
    position_size_usd: float,
    timeframe: str = "15m",
    order_type: str = "FAK",
    max_contract_price: float = 0.80,
    candle_open_ts: Optional[int] = None,
) -> dict:
    """
    Attempts a real Polymarket order. Requires POLYMARKET_PRIVATE_KEY + funder.
    For full production signing, the attached executor's place_live_order is the
    reference; this module provides a functional path that fails gracefully with
    clear errors when credentials/signing libraries are incomplete.
    """
    if not get_private_key() or not get_funder_address():
        return {
            "success": False,
            "error": "Live trading requires POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER_ADDRESS",
        }

    market = discover_market(symbol, timeframe=timeframe, anchor_ts=candle_open_ts)
    if not market:
        return {"success": False, "error": f"No Polymarket market found for {symbol} {timeframe}"}

    token_id = market["up_token_id"] if direction.upper() == "UP" else market["down_token_id"]
    if not token_id:
        return {"success": False, "error": "Token ID missing from market"}

    # ── Polymarket order semantics (official CLOB docs) ──────────────────
    # ── Sizing per official Polymarket docs ─────────────────────────────────
    # FAK/FOK BUY: amount = USD to spend (min $1). price = marketable worst-price
    #              from live order book (not a fixed GTC-style share count).
    # GTC BUY:     size = shares (book min often 5); price = limit.
    # Refs: docs.polymarket.com/trading/orders/create
    #       docs.polymarket.com/trading/place-orders (estimateMarketPrice)

    order_type = (order_type or "FAK").upper()
    if order_type not in ("FAK", "FOK", "GTC", "GTD"):
        order_type = "FAK"

    # ── Pricing: FAK ≠ GTC (from attached polymarket_executor.py) ───────────
    # FAK/FOK: market orders. Price = live best ask + small buffer so the
    #          order is marketable NOW. max_contract_price is a hard ceiling: fill only when CLOB best_ask <= cap.
    #          Empty book → skip (keep watching). Never invent fill at the cap.
    # GTC:     limit order at max_contract_price; share min from book.
    # Min for FAK is MARKET_ORDER_MIN_USD ($1), not the GTC 5-share floor.
    # Refs: docs.polymarket.com/trading/orders/create
    #       attached executor place_live_order FAK branch

    book = get_order_book(str(token_id))
    if book.get("error"):
        logger.warning("[POLY] book fetch: %s", book["error"])
    try:
        tick = float(book.get("tick_size") or market.get("tick_size") or 0.01)
    except (TypeError, ValueError):
        tick = 0.01
    if tick <= 0:
        tick = 0.01
    tick_decimals = max(
        0,
        min(8, len(str(tick).rstrip("0").split(".")[-1]) if "." in str(tick) else 2),
    )
    user_cap = min(float(max_contract_price or 0.99), 0.99)

    if order_type in ("FAK", "FOK"):
        best_ask = None
        asks = book.get("asks") or []
        if asks:
            try:
                best_ask = float(asks[0]["price"])
            except (TypeError, ValueError, KeyError, IndexError):
                best_ask = None

        # MUST have a real Polymarket best ask — never invent price at the cap
        if best_ask is None:
            err = f"no discount liquidity: empty book (no best_ask) cap={user_cap:.4f}"
            logger.info("[POLY:LIVE] skip %s %s — %s", symbol, direction, err)
            return {
                "success": False,
                "error": err,
                "debug": {"best_ask": None, "user_cap": user_cap, "book_error": book.get("error")},
            }

        if best_ask > user_cap + 1e-9:
            err = f"no discount liquidity: best_ask={best_ask:.4f} > max={user_cap:.4f}"
            logger.info(
                "[POLY:LIVE] skip %s %s — %s signer=%s funder=%s",
                symbol, direction, err, get_signer_address(), get_funder_address(),
            )
            return {
                "success": False,
                "error": err,
                "debug": {"best_ask": best_ask, "user_cap": user_cap, "asks_top": asks[:3]},
            }

        # Execute/record at the Polymarket best ask only (snap to tick, never raise to cap)
        price = float(best_ask)
        price = round(round(price / tick) * tick, tick_decimals)
        price = max(tick, min(price, user_cap))  # clamp only if float noise > cap
        logger.info(
            "[POLY:LIVE] FAK at CLOB best_ask=%.4f (cap=%.4f) tf-aware slug market",
            price, user_cap,
        )

        # Dollar notional — market order floor $1, not GTC share min
        spent = round(max(MARKET_ORDER_MIN_USD, float(position_size_usd)), 2)
        logger.info("[POLY] order spend=$%.2f (requested position_size_usd=%.2f)", spent, float(position_size_usd))
        size = round(spent / price, 4)
        if size <= 0:
            return {
                "success": False,
                "error": f"FAK size 0 (spent={spent}, price={price})",
                "debug": {"best_ask": best_ask, "asks_top": asks[:5]},
            }
    else:
        # GTC: only rest if we can place at or below cap using live best ask
        asks = book.get("asks") or []
        best_ask = None
        if asks:
            try:
                best_ask = float(asks[0]["price"])
            except (TypeError, ValueError, KeyError, IndexError):
                best_ask = None
        if best_ask is None:
            return {
                "success": False,
                "error": f"no discount liquidity: empty book for GTC cap={user_cap:.4f}",
                "debug": {"best_ask": None, "user_cap": user_cap},
            }
        if best_ask > user_cap + 1e-9:
            return {
                "success": False,
                "error": f"no discount liquidity: best_ask={best_ask:.4f} > max={user_cap:.4f}",
                "debug": {"best_ask": best_ask, "user_cap": user_cap},
            }
        # Resting limit at best_ask (not at the cap) so fill price matches book
        price = float(best_ask)
        price = round(round(price / tick) * tick, tick_decimals)
        price = max(tick, min(price, user_cap))
        logger.info("[POLY:LIVE] GTC limit at CLOB best_ask=%.4f (cap=%.4f)", price, user_cap)
        try:
            min_sh = float(book.get("min_order_size") or 5)
        except (TypeError, ValueError):
            min_sh = 5.0
        size = max(min_sh, float(position_size_usd) / price)
        size = round(size, 4)
        spent = round(price * size, 2)

    if spent <= 0:
        return {"success": False, "error": f"Invalid spent after rounding: spent={spent}"}

    logger.info(
        "[POLY:LIVE] %s %s %s type=%s spent=%.2f price=%.4f shares=%.4f slug=%s best_ask=%s",
        symbol, direction, timeframe, order_type, spent, price, size, market.get("slug"),
        (book.get("asks") or [{}])[0].get("price") if book.get("asks") else None,
    )

    try:
        return _place_signed_order(
            market=market,
            token_id=str(token_id),
            direction=direction.upper(),
            size=size,
            price=price,
            order_type=order_type,
            spent=spent,
            position_size_usd=position_size_usd,
        )
    except Exception as e:
        logger.exception("[POLY] live order failed")
        return {"success": False, "error": str(e)}


def _place_via_official_sdk(
    market, token_id, direction, size, price, order_type, spent, position_size_usd
) -> Optional[dict]:
    """
    Prefer official py-clob-client-v2 for signature types 1/2/3 (especially POLY_1271=3).
    Returns None if SDK not installed or call fails setup — caller falls back to manual path.
    """
    try:
        from py_clob_client_v2 import (
            ClobClient,
            ApiCreds,
            OrderArgs,
            OrderType,
            Side,
            SignatureTypeV2,
            PartialCreateOrderOptions,
        )
    except ImportError:
        logger.info("[POLY:SDK] py-clob-client-v2 not installed — using manual signer")
        return None

    pk = get_private_key()
    funder = get_deposit_wallet_address() or get_funder_address()
    if not pk or not funder:
        return None

    l2 = ensure_l2_creds()
    if not l2.get("success"):
        return {
            "success": False,
            "error": f"L2 credentials for SDK: {l2.get('error')}",
        }

    sig_t = get_signature_type()
    # Map int → enum when available
    try:
        sig_enum = {
            0: SignatureTypeV2.EOA,
            1: SignatureTypeV2.POLY_PROXY,
            2: SignatureTypeV2.POLY_GNOSIS_SAFE,
            3: SignatureTypeV2.POLY_1271,
        }.get(sig_t, sig_t)
    except Exception:
        sig_enum = sig_t

    try:
        creds = ApiCreds(
            api_key=l2["api_key"],
            api_secret=l2["api_secret"],
            api_passphrase=l2["api_passphrase"],
        )
        client_kwargs = {
            "host": CLOB_BASE,
            "chain_id": CHAIN_ID,
            "key": pk,
            "creds": creds,
            "signature_type": sig_enum,
            "funder": funder,
        }
        client = ClobClient(**client_kwargs)

        ot = (order_type or "FAK").upper()
        try:
            ot_enum = getattr(OrderType, ot, OrderType.FAK)
        except Exception:
            ot_enum = OrderType.FAK

        neg_risk = bool(market.get("neg_risk"))
        tick = str(market.get("tick_size") or "0.01")
        # size = shares; spent ≈ size * price
        share_size = float(size)
        if share_size <= 0 and price > 0:
            share_size = float(spent) / float(price)

        order_args = OrderArgs(
            token_id=str(token_id),
            price=float(price),
            side=Side.BUY,
            size=float(share_size),
        )
        options = PartialCreateOrderOptions(tick_size=tick, neg_risk=neg_risk)

        logger.info(
            "[POLY:SDK] create_and_post_order type=%s sig=%s funder=%s price=%.4f size=%.4f",
            ot, sig_t, funder, float(price), float(share_size),
        )
        resp = client.create_and_post_order(  # builder attribution via builderCode if SDK supports it

            order_args=order_args,
            options=options,
            order_type=ot_enum,
        )
        # Normalize response
        if not isinstance(resp, dict):
            resp = {"raw": str(resp)}
        oid = (
            resp.get("orderID")
            or resp.get("order_id")
            or resp.get("id")
            or resp.get("orderId")
        )
        status = resp.get("status") or resp.get("orderStatus") or ""
        err = resp.get("error") or resp.get("errorMsg")
        if err and not oid:
            return {"success": False, "error": f"SDK order failed: {err}"}
        if oid or str(status).upper() in ("LIVE", "MATCHED", "FILLED", "DELAYED"):
            return {
                "success": True,
                "shadow": False,
                "order_id": str(oid or status or "sdk-ok"),
                "contracts": share_size,
                "price_per_contract": float(price),
                "total_spent": float(spent),
                "slug": market.get("slug"),
                "condition_id": market.get("condition_id"),
                "token_id": str(token_id),
                "signal_direction": direction,
                "order_type": ot,
                "order_status": status,
                "signature_type_used": sig_t,
                "platform": "polymarket",
                "via": "py-clob-client-v2",
            }
        return {"success": False, "error": f"SDK unexpected response: {str(resp)[:400]}"}
    except Exception as e:
        logger.exception("[POLY:SDK] order failed")
        return {"success": False, "error": f"SDK exception: {e}"}



def _left_pad_bytes32(addr: str) -> bytes:
    a = addr.lower().replace("0x", "")
    return bytes.fromhex(a.zfill(64))


def _wrap_erc7739_deposit_wallet_sig(
    *,
    exchange: str,
    deposit: str,
    order_message: dict,
    inner_sig_hex: str,
) -> str:
    """
    ERC-7739 / TypedDataSign trailer for POLY_1271 deposit wallet orders.
    inner_sig is the ECDSA sig of TypedDataSign(contents=Order, ...DepositWallet domain fields).
    """
    from eth_account import Account
    from eth_utils import keccak
    # contents type string used by ERC-7739 (simplified Order contents)
    contents_type = (
        "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
        "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
        "uint256 timestamp,bytes32 metadata,bytes32 builder)"
    )
    # domainSeparator for DepositWallet (name, version, chainId, verifyingContract)
    # Use eth_account structured data hash when possible; fallback keccak composition
    try:
        from eth_account.messages import encode_typed_data
        # appDomainSep = hash of EIP712Domain for DepositWallet
        domain = {
            "name": "DepositWallet",
            "version": "1",
            "chainId": CHAIN_ID,
            "verifyingContract": deposit,
        }
        # Hash domain via empty message trick — compute domain separator manually
        domain_typehash = keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        name_hash = keccak(text="DepositWallet")
        version_hash = keccak(text="1")
        from eth_abi import encode as abi_encode
        domain_sep = keccak(
            abi_encode(
                ["bytes32", "bytes32", "bytes32", "uint256", "address"],
                [domain_typehash, name_hash, version_hash, CHAIN_ID, deposit],
            )
        )
        contents_typehash = keccak(text=contents_type)
        # contents hash = keccak(typehash || abi.encode fields) — use encode_typed_data struct hash
        typed = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Order": [
                    {"name": "salt", "type": "uint256"},
                    {"name": "maker", "type": "address"},
                    {"name": "signer", "type": "address"},
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "makerAmount", "type": "uint256"},
                    {"name": "takerAmount", "type": "uint256"},
                    {"name": "side", "type": "uint8"},
                    {"name": "signatureType", "type": "uint8"},
                    {"name": "timestamp", "type": "uint256"},
                    {"name": "metadata", "type": "bytes32"},
                    {"name": "builder", "type": "bytes32"},
                ],
            },
            "primaryType": "Order",
            "domain": {
                "name": "Polymarket CTF Exchange",
                "version": "2",
                "chainId": CHAIN_ID,
                "verifyingContract": exchange,
            },
            "message": order_message,
        }
        # eth_account doesn't expose struct_hash easily; use keccak of encoded order fields
        from eth_abi import encode as abi_encode
        om = order_message
        def _b32(x):
            if isinstance(x, bytes):
                return x
            s = str(x)
            if s.startswith("0x"):
                return bytes.fromhex(s[2:].zfill(64))
            return int(s).to_bytes(32, "big")
        contents_hash = keccak(
            abi_encode(
                ["bytes32", "uint256", "address", "address", "uint256", "uint256", "uint256", "uint8", "uint8", "uint256", "bytes32", "bytes32"],
                [
                    contents_typehash,
                    int(om["salt"]),
                    om["maker"],
                    om["signer"],
                    int(om["tokenId"]),
                    int(om["makerAmount"]),
                    int(om["takerAmount"]),
                    int(om["side"]),
                    int(om["signatureType"]),
                    int(om["timestamp"]),
                    _b32(om["metadata"]),
                    _b32(om["builder"]),
                ],
            )
        )
        contents_type_hex = contents_type.encode("utf-8")
        length_be2 = len(contents_type_hex).to_bytes(2, "big")
        inner = bytes.fromhex(inner_sig_hex.replace("0x", ""))
        # ERC-7739: signature || appDomainSep || contentsHash || contentsType || len
        wrapped = inner + domain_sep + contents_hash + contents_type_hex + length_be2
        return "0x" + wrapped.hex()
    except Exception as e:
        logger.warning("[POLY:1271] ERC-7739 wrap failed, using raw sig: %s", e)
        return inner_sig_hex if inner_sig_hex.startswith("0x") else "0x" + inner_sig_hex


def _session_envelope_sig(session_address: str, deposit_wallet_sig_hex: str) -> str:
    """Append session-signer envelope per Polymarket session-keys docs."""
    from eth_abi import encode as abi_encode
    signer_id = _left_pad_bytes32(session_address)
    zero = b"\x00" * 32
    inner = bytes.fromhex(deposit_wallet_sig_hex.replace("0x", ""))
    payload = abi_encode(["bytes32", "bytes32", "bytes"], [signer_id, zero, inner])
    magic = bytes.fromhex("6492649264926492649264926492649264926492649264926492649264926492")
    return "0x" + (payload + magic).hex()


def _place_signed_order(market, token_id, direction, size, price, order_type, spent, position_size_usd) -> dict:
    # Best-effort gasless trading approvals (once per process per wallet)
    try:
        ensure_trading_approvals()
    except Exception as _ap_err:
        logger.debug("approvals pre-order: %s", _ap_err)

    """Minimal V2 signed order path. Signature-type auto-fallback."""
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    from web3 import Web3

    pk = get_private_key()
    signer = get_signer_address()
    funder = get_funder_address()
    if not all([pk, signer, funder]):
        return {"success": False, "error": "Missing signer/funder"}

    # Reset one-shot re-derive flag for this order attempt
    _place_signed_order._rederived = False

    # For deposit wallet (type 3) and other smart wallets, prefer official SDK
    # which builds POLY_1271 / proxy signatures correctly.
    configured_sig = get_signature_type()
    session_key_model = bool(
        signer and funder and str(signer).lower() != str(funder).lower()
    )
    # Official SDK often missing; for session keys always use manual path with type-1 first
    if configured_sig in (1, 2, 3) and not session_key_model:
        sdk_result = _place_via_official_sdk(
            market, token_id, direction, size, price, order_type, spent, position_size_usd
        )
        if sdk_result is not None:
            return sdk_result

    # Auto-derive L2 API credentials from private key if not already set/cached
    l2 = ensure_l2_creds()
    if not l2.get("success"):
        return {
            "success": False,
            "error": f"Could not obtain L2 API credentials: {l2.get('error')}. "
                     f"Only POLYMARKET_PRIVATE_KEY + POLYMARKET_FUNDER_ADDRESS are required.",
        }
    api_key = l2["api_key"]
    api_secret = l2["api_secret"]
    api_pass = l2["api_passphrase"]

    # maker stays the deposit/funder wallet; signer is the key that derived L2 creds
    deposit = get_deposit_wallet_address() or funder
    if configured_sig == 3 and deposit:
        funder = deposit  # maker only

    # CLOB fixed-point is 6 decimals on-chain, but for market BUY:
    #   makerAmount (USDC)  → max 2 decimal places → multiple of 10_000
    #   takerAmount (shares) → max 4 decimal places → multiple of 100
    # Ref: Polymarket CLOB rejects "max accuracy of 2 decimals" on maker.
    spent_2dp = round(float(spent), 2)
    size_4dp = round(float(size), 4)
    maker_amount = int(round(spent_2dp * 1_000_000))
    taker_amount = int(round(size_4dp * 1_000_000))
    # Enforce quanta so float noise cannot produce illegal precision
    maker_amount = (maker_amount // 10_000) * 10_000
    taker_amount = (taker_amount // 100) * 100
    if maker_amount <= 0 or taker_amount <= 0:
        return {"success": False, "error": f"Invalid amounts maker={maker_amount} taker={taker_amount}"}
    ts_ms = int(time.time() * 1000)
    salt = ts_ms * 1000 + random.randint(0, 999)

    global _confirmed_sig_type
    configured = get_signature_type()
    candidates = []
    session_key_model = bool(
        signer and funder and str(signer).lower() != str(funder).lower()
    )
    if session_key_model:
        # Operator proxy (Magic): POLYMARKET_SIGNATURE_TYPE=1 → type 1 first (proven working).
        # User deposit wallet: signature_type=3 or user_creds → type 3 first (ERC-7739 + session).
        ov = _override()
        # Connected user context (thread-local private_key) = deposit wallet + session key
        is_user_deposit = bool(ov.get("private_key"))
        if is_user_deposit or configured == 3:
            # Deposit + session: ALWAYS type 3 first. Do NOT reuse operator's
            # _confirmed_sig_type (global) or user gets type 1 first → maker not allowed.
            prefer = [3, 1, 0, 2]
            candidates = list(prefer)
        else:
            # Operator / proxy: type 1 first (proven working)
            prefer = [1, 0, 2]
            if _confirmed_sig_type is not None and _confirmed_sig_type in prefer:
                candidates.append(_confirmed_sig_type)
            for tt in prefer:
                if tt not in candidates:
                    candidates.append(tt)
        logger.info(
            "[POLY:LIVE] dual-wallet model signer=%s funder=%s configured=%s user_deposit=%s candidates=%s",
            signer, funder, configured, is_user_deposit, candidates,
        )
    elif configured == 3:
        # Classic deposit wallet where private key controls the deposit flow
        candidates = [3]
    else:
        if _confirmed_sig_type is not None:
            candidates.append(_confirmed_sig_type)
        for t in (configured, 1, 2, 0, 3):
            if t not in candidates:
                candidates.append(t)
        if funder and signer and funder.lower() == signer.lower() and 0 not in candidates:
            candidates.insert(0, 0)

    exchange = (
        "0xe2222d279d744050d28e00520010520000310F59"
        if market.get("neg_risk")
        else "0xE111180000d2663C0091e4f400237545B87B996B"
    )

    domain = {
        "name": "Polymarket CTF Exchange",
        "version": "2",
        "chainId": CHAIN_ID,
        "verifyingContract": exchange,
    }
    types = {
        "Order": [
            {"name": "salt", "type": "uint256"},
            {"name": "maker", "type": "address"},
            {"name": "signer", "type": "address"},
            {"name": "tokenId", "type": "uint256"},
            {"name": "makerAmount", "type": "uint256"},
            {"name": "takerAmount", "type": "uint256"},
            {"name": "side", "type": "uint8"},
            {"name": "signatureType", "type": "uint8"},
            {"name": "timestamp", "type": "uint256"},
            {"name": "metadata", "type": "bytes32"},
            {"name": "builder", "type": "bytes32"},
        ]
    }

    last_err = None
    for sig_type in candidates:
        # Deposit wallet (type 3) + session key: maker=signer=deposit; session signs ERC-7739.
        # POLY_ADDRESS header stays the session EOA (API key owner). Docs: session-keys.
        # Proxy (type 1): maker=funder, order_signer=session/EOA.
        if sig_type == 3:
            order_signer = funder  # deposit wallet
        else:
            order_signer = signer
        order_msg = {
            "salt": salt,
            "maker": funder,
            "signer": order_signer,
            "tokenId": int(token_id),
            "makerAmount": maker_amount,
            "takerAmount": taker_amount,
            "side": 0,  # BUY
            "signatureType": sig_type,
            "timestamp": ts_ms,
            "metadata": ZERO_BYTES32,
            "builder": (get_builder_code() or ZERO_BYTES32),
        }
        try:
            if sig_type == 3:
                # POLY_1271: sign TypedDataSign(contents=Order) with session/owner key
                tds_types = {
                    "Order": types["Order"],
                    "TypedDataSign": [
                        {"name": "contents", "type": "Order"},
                        {"name": "name", "type": "string"},
                        {"name": "version", "type": "string"},
                        {"name": "chainId", "type": "uint256"},
                        {"name": "verifyingContract", "type": "address"},
                        {"name": "salt", "type": "bytes32"},
                    ],
                }
                tds_msg = {
                    "contents": order_msg,
                    "name": "DepositWallet",
                    "version": "1",
                    "chainId": CHAIN_ID,
                    "verifyingContract": funder,
                    "salt": ZERO_BYTES32,
                }
                full = {
                    "types": tds_types,
                    "primaryType": "TypedDataSign",
                    "domain": domain,  # CTF Exchange domain
                    "message": tds_msg,
                }
                signable = encode_typed_data(full_message=full)
                signed = Account.sign_message(signable, private_key=pk)
                inner = signed.signature.hex()
                if not inner.startswith("0x"):
                    inner = "0x" + inner
                signature = _wrap_erc7739_deposit_wallet_sig(
                    exchange=exchange,
                    deposit=funder,
                    order_message=order_msg,
                    inner_sig_hex=inner,
                )
                # Session envelope when signing key != deposit (session-key model)
                if str(signer).lower() != str(funder).lower():
                    signature = _session_envelope_sig(signer, signature)
            else:
                full = {"types": types, "primaryType": "Order", "domain": domain, "message": order_msg}
                signable = encode_typed_data(full_message=full)
                signed = Account.sign_message(signable, private_key=pk)
                signature = signed.signature.hex()
                if not signature.startswith("0x"):
                    signature = "0x" + signature
        except Exception as e:
            last_err = f"sign failed: {e}"
            logger.warning("[POLY:LIVE] sign failed sigType=%s: %s", sig_type, e)
            continue

        wire = {
            "salt": salt,
            "maker": funder,
            "signer": order_signer,
            "tokenId": str(token_id),
            "makerAmount": str(maker_amount),
            "takerAmount": str(taker_amount),
            "side": "BUY",
            "expiration": "0",
            "timestamp": str(ts_ms),
            "metadata": ZERO_BYTES32,
            "builder": (get_builder_code() or ZERO_BYTES32),
            "signature": signature,
            "signatureType": sig_type,
        }
        payload = {"order": wire, "owner": api_key, "orderType": order_type}
        body = json.dumps(payload, separators=(",", ":"))

        # L2 HMAC headers
        ts = str(int(time.time()))
        path = "/order"
        message = ts + "POST" + path + body
        try:
            secret = base64.urlsafe_b64decode(api_secret)
            sig = base64.urlsafe_b64encode(
                hmac.new(secret, message.encode(), hashlib.sha256).digest()
            ).decode()
        except Exception as e:
            return {"success": False, "error": f"L2 HMAC failed: {e}"}

        headers = {
            "Content-Type": "application/json",
            "POLY_ADDRESS": signer,
            "POLY_SIGNATURE": sig,
            "POLY_TIMESTAMP": ts,
            "POLY_API_KEY": api_key,
            "POLY_PASSPHRASE": api_pass,
        }
        try:
            resp = requests.post(
                f"{CLOB_BASE}/order",
                headers=headers,
                data=body,
                timeout=15,
                proxies=_proxies(),
            )
            if resp.ok:
                data = resp.json()
                oid = data.get("orderID") or data.get("order_id") or data.get("id") or str(salt)
                # Only cache confirmed type for operator (no user override) —
                # never let operator type-1 poison connected-user deposit path.
                if not _override().get("private_key"):
                    _confirmed_sig_type = sig_type
                logger.info(
                    "[POLY:LIVE] order OK id=%s status=%s sigType=%s user=%s",
                    oid, data.get("status"), sig_type, bool(_override().get("private_key")),
                )
                return {
                    "success": True,
                    "shadow": False,
                    "order_id": str(oid),
                    "contracts": size,
                    "price_per_contract": price,
                    "total_spent": spent,
                    "slug": market.get("slug"),
                    "condition_id": market.get("condition_id"),
                    "token_id": token_id,
                    "signal_direction": direction,
                    "order_type": order_type,
                    "order_status": data.get("status"),
                    "signature_type_used": sig_type,
                    "platform": "polymarket",
                }
            err = resp.text[:500]
            last_err = f"HTTP {resp.status_code}: {err}"
            logger.error(
                "[POLY:LIVE] order rejected sigType=%s %s",
                sig_type, last_err,
            )
            err_l = err.lower()

            # API key was created for a different address than order.signer
            if "signer address has to be the address of the api key" in err_l:
                last_err = (
                    last_err
                    + " | order.signer must equal API-key owner. "
                    f"signer={signer} funder={funder} order_signer={order_signer} "
                    f"l2_source={l2.get('source')} l2_signer={l2.get('signer')}"
                )
                # Type 3 sets order_signer=funder while L2 is EOA/session — expected mismatch.
                # Do NOT abort: fall through to type 1 (proxy) which matches operator.
                if sig_type == 3:
                    logger.info(
                        "[POLY:LIVE] type 3 API-key mismatch (expected for proxy) — try next candidate"
                    )
                    continue
                if not getattr(_place_signed_order, "_rederived", False):
                    _place_signed_order._rederived = True
                    logger.warning("[POLY:LIVE] forcing L2 re-derive from private key")
                    rel2 = ensure_l2_creds(force_derive=True)
                    if rel2.get("success"):
                        api_key = rel2["api_key"]
                        api_secret = rel2["api_secret"]
                        api_pass = rel2["api_passphrase"]
                        candidates.insert(0, sig_type)
                    continue
                # Keep last_err; try next signature type
                continue

            # Signature / auth issues → try next type
            if "signature" in err_l or "invalid" in err_l or resp.status_code in (401, 403):
                continue
            # Maker/deposit-wallet mismatch → try remaining types (1/2/3) before giving up
            if "maker address not allowed" in err_l or "deposit wallet" in err_l:
                continue
            # Amount/price 400s won't change with sig type
            if resp.status_code == 400:
                if "maker address not allowed" in err_l or "deposit wallet" in err_l:
                    last_err = (
                        last_err
                        + " | Fix: set POLYMARKET_FUNDER_ADDRESS to your Polymarket "
                        "deposit/proxy wallet (from polymarket.com/portfolio), "
                        "POLYMARKET_SIGNATURE_TYPE=1 (proxy) or 2 (Safe) or 3 (deposit), "
                        "and use the private key that owns that wallet."
                    )
                return {"success": False, "error": last_err}
            continue
        except Exception as e:
            last_err = str(e)
            logger.exception("[POLY:LIVE] order POST exception sigType=%s", sig_type)
            continue

    logger.error("[POLY:LIVE] all signature attempts failed: %s", last_err)
    msg = last_err or "All signature types rejected"
    if last_err and ("maker address not allowed" in last_err.lower() or "deposit wallet" in last_err.lower()):
        ov = _override()
        if ov.get("private_key"):
            msg = (
                msg
                + " | User deposit wallet: authorize this session key on Polymarket "
                "(CLOB scope) for funder="
                + str(funder)
                + ", then Mark authorized in Account. Builder API keys must be set on the server."
            )
        else:
            msg = (
                msg
                + " | Operator: POLYMARKET_FUNDER_ADDRESS must match polymarket.com/portfolio; "
                "SIGNATURE_TYPE=1 (proxy), 2 (Safe), or 3 (deposit)."
            )
    return {"success": False, "error": msg}


def execute_order(
    symbol: str,
    direction: str,
    mode: str,
    position_size_usd: float,
    timeframe: str = "15m",
    order_type: str = "FAK",
    max_contract_price: float = 0.80,
    candle_open_ts: Optional[int] = None,
    user_creds: Optional[dict] = None,
) -> dict:
    """
    user_creds: optional per-user override
      {private_key=session_key, api_key, api_secret, api_passphrase, funder, signature_type}
    When set, orders are signed/authenticated for that user only.
    """
    if user_creds:
        # Connected user: session EOA + deposit funder → POLY_1271 type 3 (deposit wallet flow)
        try:
            uc = dict(user_creds)
            pk = uc.get("private_key")
            funder = (uc.get("funder") or "").strip()
            if pk and funder:
                from eth_account import Account
                from web3 import Web3
                saddr = Account.from_key(
                    pk if str(pk).startswith("0x") else "0x" + str(pk)
                ).address
                if Web3.to_checksum_address(saddr).lower() != Web3.to_checksum_address(funder).lower():
                    uc["signature_type"] = 3
                    logger.info(
                        "[POLY:USER] deposit+session → force signature_type=3 signer=%s funder=%s",
                        saddr, funder,
                    )
            user_creds = uc
        except Exception as e:
            logger.debug("user_creds normalize: %s", e)
        push_user_creds(user_creds)
    try:
        if mode == "live":
            if not get_private_key() or not get_funder_address():
                return {
                    "success": False,
                    "error": "Live mode requires session key (or system private key) + funder",
                }
            # Always derive L2 from THIS user's session key (never operator env API keys)
            force = bool(user_creds and user_creds.get("private_key") and not (
                user_creds.get("api_key") and user_creds.get("api_secret") and user_creds.get("api_passphrase")
            ))
            l2 = ensure_l2_creds(force_derive=force)
            if not l2.get("success"):
                return {
                    "success": False,
                    "error": (
                        f"L2 credential derivation failed for signer "
                        f"{l2.get('signer') or get_signer_address()}: {l2.get('error')}"
                    ),
                }
            logger.info(
                "[POLY:USER] L2 ready source=%s signer=%s funder=%s",
                l2.get("source"), l2.get("signer"), get_funder_address(),
            )
            return place_live_order(
                symbol, direction, position_size_usd, timeframe, order_type,
                max_contract_price, candle_open_ts,
            )
        return place_shadow_order(
            symbol, direction, position_size_usd, timeframe, order_type,
            max_contract_price, candle_open_ts,
        )
    finally:
        if user_creds:
            pop_user_creds()
