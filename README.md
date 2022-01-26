# generation-diff

Streaming diffs of large ordered datasets

## Notes

Interesting semantics

- queue
  - 0-1 reader
  - 0-1 writer
  - destructive read
  - circlular
  - non-seekable
- store
  - 0-n reader
  - 0-1 writer
  - non-destructive read
  - non-shot
  - readers seekable
