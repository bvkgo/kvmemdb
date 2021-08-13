# An in-memory Key-Value Database

[![PkgGoDev](https://pkg.go.dev/badge/bvkgo/kvmemdb)](https://pkg.go.dev/github.com/bvkgo/kvmemdb)

This package implements an in-memory key-value store with support for multiple,
concurrent transactions. Database can be accessed by multiple goroutines
simultaneously, but individual transactions should only be used by one
goroutine (i.e., transactions are not thread-safe).

Database also supports user-defined key-format checkers, so that, for example,
keys can always be clean, absolute file paths. In addition, transactions
support user-defined filters, so that, transactions can be restricted to a
subdirectory path, etc.

## Example

```go
func main() {
  ctx := context.Background()

  var db DB
  tx := db.NewTx()

  old, _ := tx.Get(ctx, "/oldkey")
  _ = tx.Set(ctx, "/newkey", old + "new")
  _ = tx.Delete(ctx, "/oldkey")

  // Iterate over a range in ascending order.
  var it Iterator
  _ = tx.Ascend(ctx, "000", "999", &it)
  for k, v, err := it.GetNext(); err == nil; k, v, err = it.GetNext() {
    _ = tx.Delete(ctx, k)
  }

  if err := tx.Commit(ctx); err != nil {
    t.Fatal(err)
  }
}
```
