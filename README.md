# ds9

Zero-dependency Google Cloud Datastore client for Go. Drop-in replacement for `cloud.google.com/go/datastore` basic operations. In-memory mock implementation. Comprehensive testing.

**Why?** The official client has 50+ dependencies. `ds9` uses only Go stdlib—ideal for lightweight services and minimizing supply chain risk.

## Installation

```bash
go get github.com/codeGROOVE-dev/ds9
```

## Quick Start

This isn't the API we would choose, but our primary goal was a drop-in replacement, so usage is exactly the same as the cloud.google.com/go/datastore library:

```go
import "github.com/codeGROOVE-dev/ds9/pkg/datastore"

client, _ := datastore.NewClient(ctx, "my-project")
key := datastore.NameKey("Task", "task-1", nil)
client.Put(ctx, key, &task)
client.Get(ctx, key, &task)
```

## Migrating from cloud.google.com/go/datastore

Just switch the import path from `cloud.google.com/go/datastore` to `github.com/codeGROOVE-dev/ds9/pkg/datastore`.

## Features

**Supported Features**
- **CRUD**: Get, Put, Delete, GetMulti, PutMulti, DeleteMulti
- **Transactions**: RunInTransaction, NewTransaction, Commit, Rollback
- **Queries**: Filter, Order, Limit, Offset, Ancestor, Project, Distinct, DistinctOn, Namespace, Run (iterator), Count
- **Cursors**: Start, End, DecodeCursor
- **Keys**: NameKey, IDKey, IncompleteKey, AllocateIDs, parent keys
- **Mutations**: NewInsert, NewUpdate, NewUpsert, NewDelete, Mutate
- **Types**: string, int, int64, int32, bool, float64, time.Time, slices ([]string, []int64, []int, []float64, []bool)

**Unsupported Features**

These features are unsupported just because we haven't found a use for the feature yet. PRs welcome:

* Embedded structs, nested slices, map types, some advanced query features (streaming aggregations, OR filters).

## Testing

* Use `github.com/codeGROOVE-dev/ds9/pkg/mock` package for in-memory testing. It should work even if you choose not to use ds9.
* See [TESTING.md](TESTING.md) for integration tests.
* We aim to maintain 85% test coverage - please don't send PRs without tests.
