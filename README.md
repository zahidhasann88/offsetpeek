# offsetpeek

A read-only preview of Kafka consumer-group offset resets. It shows what
`kafka-consumer-groups.sh --reset-offsets` would do — per partition, per topic
and overall — and prints the exact command to run, without changing anything.

Built with Go, [franz-go](https://github.com/twmb/franz-go) and
[cobra](https://github.com/spf13/cobra).

## Install

```sh
go install github.com/zahidhasann88/offsetpeek/cmd/offsetpeek@latest
```

Or download a binary from the Releases page.

## Usage

```sh
offsetpeek --brokers host:9092 --group payments --to-time "2026-10-13T10:00:00+06:00" [--topic orders] [--json]
```

| Target (pick one) | Meaning |
|---|---|
| `--to-time T` | RFC3339 time, or relative to now (`-2h`, `-90m`) |
| `--to-earliest` / `--to-latest` | Earliest / latest offset |
| `--shift-by N` | Committed offset + N (negative = reprocess) |
| `--to-offset topic:partition=offset` | Explicit offset; repeatable (`partition=offset` with `--topic`) |
| `--from-file offsets.csv` | `topic,partition,offset` lines (Kafka `--from-file` format) |

| Option | Meaning |
|---|---|
| `--sample N` | Read up to N records per affected partition to estimate bytes |
| `--estimate-bytes-avg N` | Fixed bytes/message estimate instead |
| `--fail-on-active` | Exit with code 2 if the group has active members |
| `--timeout` | Overall time limit (default 30s) |
| `--sasl-mechanism`, `--sasl-user`, `--sasl-password` | SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 |
| `--tls`, `--tls-ca-file`, `--tls-cert-file`, `--tls-key-file`, `--tls-skip-verify` | TLS |

Every connection flag also reads an environment variable; see
[`.env.example`](./.env.example).

## Sample session

```
$ offsetpeek --brokers localhost:9092 --group payments --topic orders --shift-by -250 --sample 50
offsetpeek — read-only consumer-group offset reset preview
===========================================================
  brokers : localhost:9092
  group   : payments (state=Empty)
  target  : shift by -250 messages

TOPIC   PARTITION  COMMITTED  EARLIEST  LATEST  TARGET  DELTA  VERDICT
-----   ---------  ---------  --------  ------  ------  -----  -------
orders  0          260        0         260     10      -250   REPROCESS 250
orders  1          240        0         240     0       -240   REPROCESS 240
orders  2          100        0         260     0       -100   REPROCESS 100
orders  3          0          0         240     0       0      NO CHANGE

Notes
-----
  orders/1: target -10 is before earliest 0; clamped
  orders/2: target -150 is before earliest 0; clamped
  orders/3: target -250 is before earliest 0; clamped

Summary by topic
----------------
  orders                            REPROCESS 590 msgs (≈ 22.10 KiB) | 1 no-change

Overall
-------
  REPROCESS : 590 msgs (≈ 22.10 KiB)
  SKIP      : 0 msgs
  NO CHANGE : 1 partitions
  NEW       : 0 partitions
  UNRESOLVED: 0 partitions

  Estimated reprocess volume: 590 messages, ≈ 22.10 KiB (sampled up to 50 records/partition)

Equivalent kafka-consumer-groups.sh command (printed only, NOT executed by offsetpeek):
-------------------------------------------------------------------------------------
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group payments --reset-offsets --topic orders --shift-by -250 --execute
```

If the group has active members, the report starts with a warning showing
the member count and group state.

## Safety

- All Kafka access goes through an interface with no write methods.
- Tests fail if a write method is added to that interface, if the real client
  references any Kafka write API, or if a preview makes any non-read call.
- Sampling uses a consumer with no group, so it cannot commit.
- The integration test checks that committed offsets are unchanged afterwards.

Targets follow `kafka-consumer-groups.sh` rules (out-of-range targets are
clamped, a time past the last record resolves to latest), so the preview
matches what the printed command would do.

## Development

```sh
docker compose up -d                            # local Kafka with a seeded `payments` group
go run ./cmd/offsetpeek --brokers localhost:9092 --group payments --to-earliest
go run ./cmd/offsetpeek-sample                  # in-memory demo, no Docker

go test ./...                                   # unit + in-memory Kafka tests
go test -tags=integration ./internal/testutil/  # real Kafka via testcontainers (Docker)
```

On Docker Desktop for Windows, set `TESTCONTAINERS_RYUK_DISABLED=true` if
the integration test can't start its containers.

Releases are built by GoReleaser when a `v*` tag is pushed.

## License

[MIT](./LICENSE)
