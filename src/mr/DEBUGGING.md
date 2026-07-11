# MR Worker Notes

## Buffered Output

Map tasks write JSON key/value records to one intermediate file per Reduce partition. Reduce tasks write one text result line per key. Both paths use `bufio.NewWriter` before writing to an `os.File`.

Buffering accumulates small records in process memory and writes them to the file in larger batches. This reduces `write` system calls and is especially helpful when the workspace is on a mounted file system such as WSL's `/mnt/c`.

For one Map partition, the writing pattern is:

```go
file, err := os.CreateTemp(".", "mr-map-0-*")
if err != nil {
	log.Fatal(err)
}

writer := bufio.NewWriter(file)
encoder := json.NewEncoder(writer)

for _, kv := range partition {
	if err := encoder.Encode(&kv); err != nil {
		log.Fatal(err)
	}
}

if err := writer.Flush(); err != nil {
	log.Fatal(err)
}
if err := file.Close(); err != nil {
	log.Fatal(err)
}
if err := os.Rename(file.Name(), "mr-3-0"); err != nil {
	log.Fatal(err)
}
```

Without `bufio.NewWriter`, each `encoder.Encode` writes directly to `file`, which can produce many small `write` system calls. With the writer, `Encode` appends bytes to an in-memory buffer; the buffer writes a larger batch only when full or when `Flush` runs. This keeps the JSON output unchanged while reducing file-system overhead.

Publish each output file in this order:

```text
write records -> Flush -> Close -> Rename
```

`Flush` is required because buffered bytes have not reached the file before it runs. The code checks errors from `Flush`, `Close`, and `Rename` so a failed publication does not silently produce incomplete Map or Reduce output.

## Debug Logging

`debugLogging` in `worker.go` controls trace logs through `debugf`:

```go
debugLogging = false
```

Set it to `true` while debugging to print task assignment, receipt, completion, timeout, and Map timing messages. Keep it `false` for normal test runs. Errors such as failed RPC calls and `log.Fatalf` messages are not controlled by this flag and always remain visible.
