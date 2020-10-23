# gobloom

Small example of detecting duplicate keys among ~10M protobuf messages using a Bloom Filter.

## How to run

```bash
# Generate 10M protobuf messages, put them in out.ldproto
go run cmd/main.go gen-file

# Read 10M messages, use a HashMap to detect duplicates
go run cmd/main.go find-dup-with-map

# Read 10M messages, use a Bloom Filter to detect duplicates
go run cmd/main.go find-dup-with-filter
```
