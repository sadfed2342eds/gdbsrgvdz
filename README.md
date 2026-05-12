# GoChain address activity checker

Многопоточный батчевый чеккер адресов под **GoChain** (chain id 60) на Go.
Читает список адресов из файла, бьёт напрямую в публичную JSON-RPC ноду
`https://rpc.gochain.io` и пишет активные адреса в `result.txt`.

## Что считается активностью

Поскольку это обычная JSON-RPC нода (без индексации истории токен-трансферов),
адрес считается активным, если выполнено хотя бы одно:

- `eth_getTransactionCount(addr, "latest") > 0` — адрес **отправлял** хотя бы
  одну транзакцию (нативка, ERC-20, NFT, вызов контракта — что угодно),
- `eth_getBalance(addr, "latest") > 0` — на адрес **приходила** нативка.

Этого достаточно, чтобы отсечь «пустышки» среди своих кошельков.

## Особенности

- Пул воркеров (`--workers`).
- Глобальный rate limiter — token bucket (`--rps`).
- Ретраи с exponential backoff + jitter (`--retries`).
- Short-circuit: как только первый признак активности найден, второй вызов
  не делается.
- Только stdlib, без внешних зависимостей.
- Дедуп и валидация адресов на входе.
- Конкурентная запись в `result.txt` с flush на каждую строку.
- Graceful shutdown по Ctrl+C.

## Использование

```bash
# addresses.txt — по одному адресу на строку
go run . \
  --in addresses.txt \
  --out result.txt \
  --rpc https://rpc.gochain.io \
  --workers 20 \
  --rps 20 \
  --retries 5
```

## Сборка

```bash
go build -o checker .
./checker --in addresses.txt
```

Если публичная нода начнёт резать по лимитам — снижай `--rps` / `--workers`
или подставь свой приватный RPC через `--rpc`.
