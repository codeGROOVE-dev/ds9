# Testing Guide

## Unit Tests

Run the standard test suite with mock Datastore:

```bash
make test
```

This runs all tests including integration tests against an in-memory mock server. No GCP credentials required.

## Integration Tests

Integration tests automatically use the mock server by default, but can run against real Google Cloud Datastore when `DS9_TEST_PROJECT` is set.

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
1. Check if the test database (`ds9-test`) exists, or create it if needed
2. If creating a new database, wait 10 seconds for it to propagate (GCP needs time to make the database available)
3. Run the full integration test suite (including cleanup of test entities)
4. Retain the database for reuse in subsequent test runs

**Customization:**
```bash
# Use a different project
make integration DS9_TEST_PROJECT=my-project

# Use a different database name
make integration DS9_TEST_DATABASE=my-test-db

# Use a different location
make integration DS9_TEST_LOCATION=europe-west1
```

### How It Works

- **Without `DS9_TEST_PROJECT`**: Tests run against an in-memory mock server (fast, no GCP needed)
- **With `DS9_TEST_PROJECT`**: Tests run against real Google Cloud Datastore (requires GCP credentials)

The same test code runs in both modes, ensuring the mock accurately represents real Datastore behavior.

### What Gets Tested

- **Basic Operations**: Put, Get, Update, Delete
- **Batch Operations**: PutMulti, GetMulti, DeleteMulti
- **Transactions**: Read-modify-write operations
- **Queries**: KeysOnly queries with limits
- **Cleanup**: DeleteAllByKind operation

### Test Data

Test entities use the kind `DS9IntegrationTest` with unique timestamp-based names to avoid conflicts. Each test run creates entities, and the final cleanup test deletes all entities of this kind.

### Database Cleanup

The test database is retained between test runs for performance (database creation takes several minutes). Test entities are automatically cleaned up at the end of each test run.

To manually delete the test database:

```bash
# Delete test database
make clean-integration-db

# Or use gcloud directly
gcloud firestore databases delete --database=ds9-test --project=integration-testing-476513
```

### Costs

Integration tests create or reuse a persistent database and a small number of entities (typically <20 per run). Entities are deleted after each test run. The persistent database incurs minimal costs and should fall well within GCP free tier limits.
