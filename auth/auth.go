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
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	maxBodySize    = 10 * 1024 * 1024 // 10MB
	defaultTimeout = 30 * time.Second
	metadataFlavor = "Google"
	//nolint:revive // GCP metadata server only supports HTTP
	defaultMetadataURL = "http://metadata.google.internal/computeMetadata/v1"
)

var httpClient = &http.Client{
	Timeout: defaultTimeout,
}

// Config holds auth configuration.
type Config struct {
	// MetadataURL is the URL for the GCP metadata server.
	// Defaults to the production metadata server if empty.
	MetadataURL string

	// SkipADC skips Application Default Credentials and goes straight to metadata server.
	// Useful for testing to ensure mock servers are used.
	SkipADC bool
}

// configKey is the key for storing Config in context.
type configKey struct{}

// WithConfig returns a new context with the given auth config.
func WithConfig(ctx context.Context, cfg *Config) context.Context {
	return context.WithValue(ctx, configKey{}, cfg)
}

// getConfig retrieves the auth config from context, or returns defaults.
func getConfig(ctx context.Context) *Config {
	if cfg, ok := ctx.Value(configKey{}).(*Config); ok && cfg != nil {
		return cfg
	}
	return &Config{
		MetadataURL: defaultMetadataURL,
		SkipADC:     false,
	}
}

// AccessToken retrieves a GCP access token.
// It tries Application Default Credentials first, then falls back to the metadata server.
// Configuration can be provided via auth.WithConfig in the context.
func AccessToken(ctx context.Context) (string, error) {
	cfg := getConfig(ctx)

	// Skip ADC if configured (useful for testing to ensure mock metadata server is used)
	if !cfg.SkipADC {
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

	// Use url.Values for proper URL encoding to prevent parameter injection
	form := url.Values{
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}
	reqBody := form.Encode()

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
		// Log full error details but return sanitized message to prevent information leakage
		slog.ErrorContext(ctx, "OAuth token exchange failed", "status", resp.StatusCode, "response", string(body))
		return "", fmt.Errorf("token exchange returned %d", resp.StatusCode)
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
	cfg := getConfig(ctx)
	reqURL := cfg.MetadataURL + "/instance/service-accounts/default/token"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
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
	cfg := getConfig(ctx)
	reqURL := cfg.MetadataURL + "/project/project-id"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
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
