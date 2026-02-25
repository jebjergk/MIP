# Intraday v2 Backfill Runbook Notes

## Chunk with zero bars but non-zero trust rows

- A chunk can legitimately show `ROWS_STATE_SNAPSHOT = 0` and `ROWS_TRUST > 0`.
- Reason: trust snapshots are computed from the rolling training window ending at chunk end (`TRAIN_WINDOW_END`), not only from bars inside that specific chunk.
- If a chunk has no new bars/signals, trust still materializes because prior-window outcomes exist and are re-evaluated at that snapshot timestamp.
- This is expected behavior and is not a data integrity issue.
