# Testing Guide

## Unit Tests

Run the standard test suite with mock Datastore:

```bash
make test
```

This uses `ds9mock` for in-memory testing without requiring GCP credentials.

## Integration Tests

Integration tests run against real Google Cloud Datastore and automatically manage database lifecycle.

### Prerequisites

1. **Authenticate with gcloud:**
   ```bash
   gcloud auth application-default login
   ```

2. **Ensure the test project exists:**
   - Default Project: `integration-testing-476513`
   - Override with: `make integration DS9_TEST_PROJECT=your-project`
   - Cloud Firestore API must be enabled
   - You must have permissions: `datastore.databases.*, datastore.entities.*`

3. **Verify access:**
   ```bash
   gcloud config set project integration-testing-476513
   gcloud firestore databases list
   ```

### Running Integration Tests

```bash
make integration
```

This will automatically:
1. Create a temporary Datastore database (`ds9-test`)
2. Run the full integration test suite
3. Delete the temporary database (even if tests fail)

**Customization:**
```bash
# Use a different project
make integration DS9_TEST_PROJECT=my-project

# Use a different database name
make integration DS9_TEST_DATABASE=my-test-db

# Use a different location
make integration DS9_TEST_LOCATION=europe-west1
```

### What Gets Tested

- **Basic Operations**: Put, Get, Update, Delete
- **Batch Operations**: PutMulti, GetMulti, DeleteMulti
- **Transactions**: Read-modify-write operations
- **Queries**: KeysOnly queries with limits

### Test Data

Test entities use the kind `DS9IntegrationTest` with unique timestamp-based names to avoid conflicts. The test database (`ds9-test`) is automatically created before tests and deleted after, ensuring complete cleanup.

### Manual Cleanup

The Makefile automatically cleans up the test database, even if tests fail. If you need to manually clean up:

```bash
# List databases
gcloud firestore databases list --project=integration-testing-476513

# Delete test database if it exists
gcloud firestore databases delete --database=ds9-test --project=integration-testing-476513
```

### Costs

Integration tests create a temporary database and a small number of entities (typically <20 per run). The database and all data are deleted after the test run completes. This should fall well within GCP free tier limits.
