package datastore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	neturl "net/url"
	"time"
)

// doRequest performs an HTTP request with exponential backoff retries.
// Returns an error if the status code is not 200 OK.
func doRequest(ctx context.Context, logger *slog.Logger, url string, jsonData []byte, token, projectID, databaseID string) ([]byte, error) {
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms... capped at maxBackoffMS
			backoffMS := math.Min(float64(baseBackoffMS)*math.Pow(2, float64(attempt-1)), float64(maxBackoffMS))
			// Add jitter: ±25% randomness
			jitter := backoffMS * jitterFraction * (2*rand.Float64() - 1) //nolint:gosec // Weak random is acceptable for jitter
			sleepMS := backoffMS + jitter
			sleepDuration := time.Duration(sleepMS) * time.Millisecond

			logger.DebugContext(ctx, "retrying request",
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"backoff_ms", int(sleepMS),
				"last_error", lastErr)

			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		// Add routing header for named databases
		if databaseID != "" {
			// URL-encode values to prevent header injection attacks
			routingHeader := fmt.Sprintf("project_id=%s&database_id=%s", neturl.QueryEscape(projectID), neturl.QueryEscape(databaseID))
			req.Header.Set("X-Goog-Request-Params", routingHeader)
		}

		logger.DebugContext(ctx, "sending request", "url", url, "attempt", attempt+1)

		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			logger.WarnContext(ctx, "request failed", "error", err, "attempt", attempt+1)
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, err)
			}
			continue
		}

		// Always close response body
		defer func() { //nolint:revive,gocritic // Defer in loop is intentional - loop exits after successful response
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.WarnContext(ctx, "failed to close response body", "error", closeErr)
			}
		}()

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
		if err != nil {
			lastErr = err
			logger.WarnContext(ctx, "failed to read response body", "error", err, "attempt", attempt+1)
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("failed to read response after %d attempts: %w", maxRetries, err)
			}
			continue
		}

		logger.DebugContext(ctx, "received response",
			"status_code", resp.StatusCode,
			"body_size", len(body),
			"attempt", attempt+1)

		// Success
		if resp.StatusCode == http.StatusOK {
			return body, nil
		}

		// Don't retry on 4xx errors (client errors)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			if resp.StatusCode == http.StatusNotFound {
				logger.DebugContext(ctx, "entity not found", "status_code", resp.StatusCode)
			} else {
				logger.WarnContext(ctx, "client error", "status_code", resp.StatusCode, "body", string(body))
			}
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Unexpected 2xx/3xx status codes
		if resp.StatusCode < 400 {
			logger.WarnContext(ctx, "unexpected non-200 success status", "status_code", resp.StatusCode)
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
		}

		// 5xx errors - retry
		lastErr = fmt.Errorf("server error: status %d", resp.StatusCode)
		logger.WarnContext(ctx, "server error, will retry",
			"status_code", resp.StatusCode,
			"attempt", attempt+1,
			"body", string(body))
	}

	return nil, fmt.Errorf("all %d attempts failed: %w", maxRetries, lastErr)
}
