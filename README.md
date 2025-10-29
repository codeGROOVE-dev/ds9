# ds9

Zero-dependency Google Cloud Datastore client for Go. Drop-in replacement for `cloud.google.com/go/datastore` basic operations.

**Why?** The official client has 50+ dependencies. `ds9` uses only Go stdlib—ideal for lightweight services and minimizing supply chain risk.

## Installation

```bash
go get github.com/codeGROOVE-dev/ds9
```

## Quick Start

```go
import "github.com/codeGROOVE-dev/ds9"

client, _ := ds9.NewClient(ctx, "my-project")
key := ds9.NameKey("Task", "task-1", nil)
client.Put(ctx, key, &task)
client.Get(ctx, key, &task)
```

**Supported:**
- **CRUD**: Get, Put, Delete, GetMulti, PutMulti, DeleteMulti
- **Transactions**: RunInTransaction, NewTransaction, Commit, Rollback
- **Queries**: Filter, Order, Limit, Offset, Ancestor, Project, Distinct, DistinctOn, Namespace, Run (iterator), Count
- **Cursors**: Start, End, DecodeCursor
- **Keys**: NameKey, IDKey, IncompleteKey, AllocateIDs, parent keys
- **Mutations**: NewInsert, NewUpdate, NewUpsert, NewDelete, Mutate
- **Types**: string, int, int64, int32, bool, float64, time.Time, slices ([]string, []int64, []int, []float64, []bool)

## Migrating from Official Client

Change the import—API is compatible:
```go
// import "cloud.google.com/go/datastore"
import "github.com/codeGROOVE-dev/ds9"
```

Use `ds9mock` package for in-memory testing. See [TESTING.md](TESTING.md) for integration tests.

## Limitations

Not supported: embedded structs, nested slices, map types, some advanced query features (streaming aggregations, OR filters).

See [example/](example/) for usage. Apache 2.0 licensed.
