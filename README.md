# GoChain address activity checker

Многопоточный батчевый чеккер адресов под **GoChain** (chain id 60) на Go.
Читает список адресов из файла, бьёт напрямую в публичную JSON-RPC ноду
`https://rpc.gochain.io` и пишет активные адреса в `result.txt`.

## Что считается активностью

Поскольку это обычная JSON-RPC нода, активность определяется тремя дешёвыми
проверками (short-circuit — останавливаемся на первом «да»):

1. `eth_getTransactionCount(addr, "latest") > 0` — адрес хоть раз
   **отправлял** tx (нативка, ERC-20/721/1155, вызов контракта).
2. `eth_getBalance(addr, "latest") > 0` — на адрес приходила нативка.
3. `eth_getLogs` на `Transfer` / `TransferSingle` / `TransferBatch` с фильтром
   по теме-получателю (padded address) — ловит **только получавшие** кошельки
   (ERC-20, ERC-721, ERC-1155), в том числе те, что потом были выведены в ноль.

Третий шаг опциональный (`--logs=true`, по умолчанию включён). Если нода
отказывает в широком диапазоне блоков, скрипт автоматически переходит в
чанкование с половинным делением окна.

## Особенности

- Пул воркеров (`--workers`).
- Глобальный rate limiter — token bucket (`--rps`).
- Ретраи с exponential backoff + jitter (`--retries`).
- Short-circuit: как только первый признак активности найден, следующие
  вызовы не делаются.
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
  --retries 5 \
  --logs \
  --chunk 0
```

Флаги:
- `--logs` — включает 3-й шаг (eth_getLogs). Выключи (`--logs=false`), если
  публичная нода медленная или тебе хватает first-two checks.
- `--chunk` — размер окна блоков для `eth_getLogs` (0 = сперва пробуем
  `[0, latest]` одним запросом, чанкуем только при ошибке «range too wide»).

## Сборка

```bash
go build -o checker .
./checker --in addresses.txt
```

Если публичная нода начнёт резать по лимитам — снижай `--rps` / `--workers`
или подставь свой приватный RPC через `--rpc`.
