# EVM address activity checker

Многопоточный батчевый чеккер EVM-адресов на Go. Берёт список адресов,
проверяет каждый через Etherscan V2 unified API (нативные tx + ERC-20 + ERC-721)
и пишет активные в `result.txt`.

## Особенности

- Пул воркеров с настраиваемым параллелизмом (`--workers`).
- Глобальный rate limiter (`--rps`), чтобы не ловить 429.
- Ретраи с exponential backoff + jitter (`--retries`).
- Short-circuit: как только найдена любая активность — адрес записан, следующие
  запросы по нему не делаются.
- Дедупликация входа и валидация адресов.
- Безопасная конкурентная запись в `result.txt` (flush на каждую запись).
- Graceful shutdown по Ctrl+C.

## Использование

```bash
export ETHERSCAN_API_KEY=...   # ключ Etherscan V2 (один на все EVM-сети)

# addresses.txt — по одному адресу на строку
go run . \
  --in addresses.txt \
  --out result.txt \
  --chain 1 \
  --workers 10 \
  --rps 5 \
  --retries 5
```

Чейны (chain id): 1 = Ethereum, 56 = BSC, 137 = Polygon, 42161 = Arbitrum,
10 = Optimism, 8453 = Base, 43114 = Avalanche и т.д. Полный список —
в [документации Etherscan V2](https://docs.etherscan.io/etherscan-v2/).

На free-плане Etherscan лимит = 5 req/s, поэтому по умолчанию `--rps=5`.
На платных — можно повышать и увеличивать `--workers`.

## Сборка

```bash
go build -o checker .
./checker --in addresses.txt
```
