# Managed deposit wallets (connected users)

## Isolation from admin

| Role | Keys | Tables |
|------|------|--------|
| **Admin / operator** | `.env` `POLYMARKET_PRIVATE_KEY` + `FUNDER` | `settings` + `signals` |
| **Connected user** | `user_credentials` only (owner + session + deposit) | `user_trades` + `user_settings` |

Admin live path never reads `user_credentials`. User fan-out never uses operator private key.

## Flow (Setup managed wallet)

1. Create **owner** EOA → encrypted as `owner_key_enc` (user can export later)
2. **WALLET-CREATE** via Builder Relayer (`from=owner`) → deposit funder
3. Create **session** EOA → trade-only
4. Authorize session with **owner Batch signature + Builder HMAC**

Live only when `session_authorized=True` after step 4 succeeds.

## Server env (operator)

```
POLYMARKET_BUILDER_API_KEY=...
POLYMARKET_BUILDER_SECRET=...
POLYMARKET_BUILDER_PASSPHRASE=...
```

Builder must be allowed to authorize session keys (contact builder@polymarket.com if 403 persists).

## User steps

1. Log in as connected user (not admin)
2. Account → **Setup managed wallet**
3. Fund the returned **deposit** address with pUSD
4. Settings → Live

## Existing polymarket.com wallets

Paste funder + generate session still available, but Builder often returns **403 not allowed** for wallets not created by your Builder. Prefer managed path.
