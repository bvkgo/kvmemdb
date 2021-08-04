# An in-memory Key-Value Database

This package implements an in-memory key-value store with support for multiple,
concurrent transactions. Database can be accessed by multiple goroutines
simultaneously, but individual transactions should only be used by one
goroutine (i.e., trasactions are not thread-safe).

Database also supports user-defined key-format checkers, so that, for example,
keys can be absolute file paths. In addition, transactions support user-defined
filters, so that, for example, transactions can be restricted to a subdirectory
path, etc.

## Example

```go
func main() {
  ctx := context.Background()

  var db DB
  tx := db.NewTx()

  old, _ := tx.Get(ctx, "/oldkey")
  _ = tx.Set(ctx, "/newkey", old + "new")
  _ = tx.Delete(ctx, "/oldkey")

  // Loop over all files inside /tmp directory using greater-than search repeatedly.
  prefix := "/tmp/"
  for {
    tmpFile, tmpValue, err := tx.FindGT(ctx, prefix);
    if err != nil || !strings.HasPrefix(tmpFile, "/tmp/") {
      break
    }
    prefix = tmpFile
  }

  // Scan all key-value pairs in the database.
  _ = tx.Scan(ctx, func(_ context.Context, k, v string) error {
    log.Println(k, v)
    return nil
  })

  // Iterate over a range in ascending order.
  tx.Ascend(ctx, "000", "999", func(_ context.context, k, _ string) error {
    _ = tx.Delete(ctx, k)
    return nil
  })

  if err := tx.Commit(ctx); err != nil {
    t.Fatal(err)
  }
}
```
