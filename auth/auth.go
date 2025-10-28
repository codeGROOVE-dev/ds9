// Package auth provides authentication for Google Cloud APIs.
//
// It supports multiple authentication methods:
// - Application Default Credentials (for local development via gcloud).
// - GCP Metadata Server (for GCE, GKE, Cloud Run, Cloud Functions, App Engine).
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	maxBodySize    = 10 * 1024 * 1024 // 10MB
	defaultTimeout = 30 * time.Second
	metadataFlavor = "Google"
)

var (
	metadataURL = "http://metadata.google.internal/computeMetadata/v1" //nolint:revive // GCP metadata server only supports HTTP
	isTestMode  = false

	httpClient = &http.Client{
		Timeout: defaultTimeout,
	}
)

// SetMetadataURL sets a custom metadata server URL for testing.
// Returns a function that restores the original URL.
func SetMetadataURL(url string) func() {
	old := metadataURL
	oldTestMode := isTestMode
	metadataURL = url
	isTestMode = true // Enable test mode to skip ADC
	return func() {
		metadataURL = old
		isTestMode = oldTestMode
	}
}

// AccessToken retrieves a GCP access token.
// It tries Application Default Credentials first, then falls back to the metadata server.
// In test mode, ADC is skipped to ensure mock servers are used.
func AccessToken(ctx context.Context) (string, error) {
	// Skip ADC in test mode to ensure tests use mock metadata server
	if !isTestMode {
		// Try Application Default Credentials first (for local development)
		token, err := accessTokenFromADC(ctx)
		if err == nil {
			return token, nil
		}
	}

	// Fall back to metadata server (for GCP environments or tests)
	return accessTokenFromMetadata(ctx)
}

// accessTokenFromADC retrieves an access token from Application Default Credentials.
// This supports gcloud auth application-default login for local development.
func accessTokenFromADC(ctx context.Context) (string, error) {
	// Check GOOGLE_APPLICATION_CREDENTIALS environment variable
	credsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credsFile == "" {
		// Check well-known ADC location
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		credsFile = homeDir + "/.config/gcloud/application_default_credentials.json"
	}

	// Read credentials file
	data, err := os.ReadFile(credsFile)
	if err != nil {
		return "", fmt.Errorf("failed to read credentials file: %w", err)
	}

	// Parse credentials
	var creds struct {
		Type         string `json:"type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`
	}

	if err := json.Unmarshal(data, &creds); err != nil {
		return "", fmt.Errorf("failed to parse credentials: %w", err)
	}

	// Only support authorized_user type (from gcloud auth application-default login)
	if creds.Type != "authorized_user" {
		return "", fmt.Errorf("unsupported credential type: %s", creds.Type)
	}

	// Exchange refresh token for access token
	return exchangeRefreshToken(ctx, creds.ClientID, creds.ClientSecret, creds.RefreshToken)
}

// exchangeRefreshToken exchanges a refresh token for an access token.
func exchangeRefreshToken(ctx context.Context, clientID, clientSecret, refreshToken string) (string, error) {
	tokenURL := "https://oauth2.googleapis.com/token" //nolint:gosec // This is Google's OAuth2 token endpoint, not a hardcoded credential

	reqBody := fmt.Sprintf(
		"client_id=%s&client_secret=%s&refresh_token=%s&grant_type=refresh_token",
		clientID, clientSecret, refreshToken,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(reqBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token exchange failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.WarnContext(ctx, "failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
		if readErr != nil {
			return "", fmt.Errorf("token exchange returned %d", resp.StatusCode)
		}
		return "", fmt.Errorf("token exchange returned %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return "", err
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}

	return tokenResp.AccessToken, nil
}

// accessTokenFromMetadata retrieves an access token from the GCP metadata server.
// This is used when running on GCP (GCE, GKE, Cloud Run, etc.).
func accessTokenFromMetadata(ctx context.Context) (string, error) {
	url := metadataURL + "/instance/service-accounts/default/token"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", metadataFlavor)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.WarnContext(ctx, "failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return "", err
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token: %w", err)
	}

	return tokenResp.AccessToken, nil
}

// ProjectID retrieves the project ID from the GCP metadata server.
func ProjectID(ctx context.Context) (string, error) {
	url := metadataURL + "/project/project-id"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", metadataFlavor)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.WarnContext(ctx, "failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return "", err
	}

	return string(body), nil
}
