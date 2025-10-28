// Package main demonstrates basic usage of the ds9 Datastore client.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/codeGROOVE-dev/ds9"
)

// Task represents a simple task entity.
type Task struct {
	CreatedAt   time.Time `datastore:"created_at"`
	Title       string    `datastore:"title"`
	Description string    `datastore:"description,noindex"`
	Priority    int64     `datastore:"priority"`
	Done        bool      `datastore:"done"`
}

func main() {
	ctx := context.Background()

	// Create a new Datastore client
	// The project ID will be automatically detected from the GCP metadata server
	client, err := ds9.NewClient(ctx, "my-project")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Example 1: Put a new entity
	task := &Task{
		Title:       "Write README",
		Description: "Document the ds9 library",
		Done:        false,
		Priority:    1,
		CreatedAt:   time.Now(),
	}

	key := ds9.NameKey("Task", "task-1", nil)
	_, err = client.Put(ctx, key, task)
	if err != nil {
		log.Fatalf("Failed to put task: %v", err)
	}
	fmt.Println("Task created successfully")

	// Example 2: Get an entity
	var retrieved Task
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		log.Fatalf("Failed to get task: %v", err)
	}
	fmt.Printf("Retrieved task: %s (Done: %v)\n", retrieved.Title, retrieved.Done)

	// Example 3: Update the entity
	retrieved.Done = true
	_, err = client.Put(ctx, key, &retrieved)
	if err != nil {
		log.Fatalf("Failed to update task: %v", err)
	}
	fmt.Println("Task updated successfully")

	// Example 4: Query for all task keys
	query := ds9.NewQuery("Task").KeysOnly().Limit(100)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		log.Fatalf("Failed to query tasks: %v", err)
	}
	fmt.Printf("Found %d tasks\n", len(keys))

	// Example 5: Use a transaction
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var current Task
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		// Toggle the done status
		current.Done = !current.Done

		_, err := tx.Put(key, &current)
		return err
	})
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}
	fmt.Println("Transaction completed successfully")

	// Example 6: Delete an entity
	err = client.Delete(ctx, key)
	if err != nil {
		log.Fatalf("Failed to delete task: %v", err)
	}
	fmt.Println("Task deleted successfully")

	// Example 7: Check that entity is gone
	err = client.Get(ctx, key, &retrieved)
	if errors.Is(err, ds9.ErrNoSuchEntity) {
		fmt.Println("Confirmed: task no longer exists")
	} else if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
}
