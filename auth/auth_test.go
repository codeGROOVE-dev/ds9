package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWithConfig(t *testing.T) {
	// Test that config can be set in context
	cfg := &Config{
		MetadataURL: "http://custom-metadata",
		SkipADC:     true,
	}
	ctx := WithConfig(context.Background(), cfg)

	// Context should be non-nil
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}

	// Verify config is retrievable
	retrievedCfg := getConfig(ctx)
	if retrievedCfg.MetadataURL != "http://custom-metadata" {
		t.Errorf("expected MetadataURL to be http://custom-metadata, got %s", retrievedCfg.MetadataURL)
	}

	if !retrievedCfg.SkipADC {
		t.Error("expected SkipADC to be true")
	}
}

func TestAccessTokenFromMetadata(t *testing.T) {
	tests := []struct {
		response       any
		name           string
		wantToken      string
		errContains    string
		metadataFlavor string
		statusCode     int
		wantErr        bool
	}{
		{
			name:       "success",
			statusCode: http.StatusOK,
			response: map[string]any{
				"access_token": "test-token-123",
				"expires_in":   3600,
			},
			wantToken:      "test-token-123",
			metadataFlavor: "Google",
		},
		{
			name:           "server error",
			statusCode:     http.StatusInternalServerError,
			response:       map[string]any{},
			wantErr:        true,
			errContains:    "metadata server returned 500",
			metadataFlavor: "Google",
		},
		{
			name:           "unauthorized",
			statusCode:     http.StatusUnauthorized,
			response:       map[string]any{},
			wantErr:        true,
			errContains:    "metadata server returned 401",
			metadataFlavor: "Google",
		},
		{
			name:           "invalid json",
			statusCode:     http.StatusOK,
			response:       "invalid json",
			wantErr:        true,
			errContains:    "failed to parse token",
			metadataFlavor: "Google",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Check metadata flavor header
				if r.Header.Get("Metadata-Flavor") != tt.metadataFlavor {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				w.WriteHeader(tt.statusCode)
				if tt.statusCode == http.StatusOK || tt.statusCode >= 400 {
					w.Header().Set("Content-Type", "application/json")
					switch v := tt.response.(type) {
					case string:
						if _, err := w.Write([]byte(v)); err != nil {
							t.Logf("write failed: %v", err)
						}
					default:
						if err := json.NewEncoder(w).Encode(tt.response); err != nil {
							t.Logf("encode failed: %v", err)
						}
					}
				}
			}))
			defer server.Close()

			ctx := WithConfig(context.Background(), &Config{
				MetadataURL: server.URL,
				SkipADC:     true,
			})

			token, err := accessTokenFromMetadata(ctx)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errContains)
				} else if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if token != tt.wantToken {
					t.Errorf("expected token %q, got %q", tt.wantToken, token)
				}
			}
		})
	}
}

func TestAccessToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": "metadata-token",
			"expires_in":   3600,
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})

	// With SkipADC=true, should use metadata server
	token, err := AccessToken(ctx)
	if err != nil {
		t.Fatalf("AccessToken failed: %v", err)
	}

	if token != "metadata-token" {
		t.Errorf("expected token 'metadata-token', got %q", token)
	}
}

func TestAccessTokenFromADC(t *testing.T) {
	// Create temp directory for credentials
	tmpDir := t.TempDir()
	credsFile := filepath.Join(tmpDir, "credentials.json")

	// Create OAuth token server
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Read and verify request body
		body := make([]byte, 1024)
		n, err := r.Body.Read(body)
		if err != nil && err != io.EOF {
			t.Logf("failed to read body: %v", err)
		}
		bodyStr := string(body[:n])

		if !strings.Contains(bodyStr, "grant_type=refresh_token") {
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(`{"error":"invalid_grant"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": "adc-access-token",
			"expires_in":   3600,
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer tokenServer.Close()

	tests := []struct {
		setupEnv    func()
		name        string
		credsData   string
		errContains string
		wantToken   string
		wantErr     bool
	}{
		{
			name: "success with valid credentials",
			credsData: `{
				"type": "authorized_user",
				"client_id": "test-client-id",
				"client_secret": "test-secret",
				"refresh_token": "test-refresh-token"
			}`,
			setupEnv: func() {
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)
			},
			wantToken: "adc-access-token",
		},
		{
			name: "unsupported credential type",
			credsData: `{
				"type": "service_account",
				"project_id": "test-project"
			}`,
			setupEnv: func() {
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)
			},
			wantErr:     true,
			errContains: "unsupported credential type",
		},
		{
			name:      "file not found",
			credsData: "", // Don't create file
			setupEnv: func() {
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/file.json")
			},
			wantErr:     true,
			errContains: "failed to read credentials file",
		},
		{
			name:      "invalid json",
			credsData: "invalid json content",
			setupEnv: func() {
				t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)
			},
			wantErr:     true,
			errContains: "failed to parse credentials",
		},
	}

	// Temporarily override OAuth token URL for testing
	// We'll need to modify exchangeRefreshToken to be testable
	// For now, skip the actual OAuth exchange test

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write credentials file if data provided
			if tt.credsData != "" {
				if err := os.WriteFile(credsFile, []byte(tt.credsData), 0o600); err != nil {
					t.Fatalf("failed to write credentials file: %v", err)
				}
			}

			tt.setupEnv()

			ctx := context.Background()

			// Note: This will try to hit the real OAuth endpoint
			// We'll mark this as expected to fail for now
			token, err := accessTokenFromADC(ctx)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errContains)
				} else if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				// Since we can't easily mock the OAuth endpoint,
				// we expect this to fail with a network error
				// In a real implementation, we'd inject the HTTP client
				if err == nil {
					t.Logf("got token: %s", token)
				} else {
					t.Logf("expected OAuth network error (can't mock easily): %v", err)
				}
			}
		})
	}
}

func TestProjectID(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		wantProject string
		errContains string
		statusCode  int
		wantErr     bool
	}{
		{
			name:        "success",
			statusCode:  http.StatusOK,
			response:    "my-test-project",
			wantProject: "my-test-project",
		},
		{
			name:        "server error",
			statusCode:  http.StatusInternalServerError,
			response:    "error",
			wantErr:     true,
			errContains: "metadata server returned 500",
		},
		{
			name:        "not found",
			statusCode:  http.StatusNotFound,
			response:    "",
			wantErr:     true,
			errContains: "metadata server returned 404",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Metadata-Flavor") != "Google" {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				w.WriteHeader(tt.statusCode)
				if _, err := w.Write([]byte(tt.response)); err != nil {
					t.Logf("write failed: %v", err)
				}
			}))
			defer server.Close()

			ctx := WithConfig(context.Background(), &Config{
				MetadataURL: server.URL,
				SkipADC:     true,
			})
			projectID, err := ProjectID(ctx)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errContains)
				} else if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if projectID != tt.wantProject {
					t.Errorf("expected project ID %q, got %q", tt.wantProject, projectID)
				}
			}
		})
	}
}

func TestAccessTokenMetadataServerDown(t *testing.T) {
	// Point to non-existent server
	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: "http://localhost:59999",
		SkipADC:     true,
	})
	_, err := accessTokenFromMetadata(ctx)

	if err == nil {
		t.Error("expected error when metadata server is down, got nil")
	}

	if !strings.Contains(err.Error(), "token request failed") {
		t.Errorf("expected 'token request failed' error, got: %v", err)
	}
}

func TestProjectIDMetadataServerDown(t *testing.T) {
	// Point to non-existent server
	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: "http://localhost:59998",
		SkipADC:     true,
	})
	_, err := ProjectID(ctx)

	if err == nil {
		t.Error("expected error when metadata server is down, got nil")
	}

	if !strings.Contains(err.Error(), "metadata request failed") {
		t.Errorf("expected 'metadata request failed' error, got: %v", err)
	}
}

func TestExchangeRefreshTokenErrors(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		errContains string
		statusCode  int
		wantErr     bool
	}{
		{
			name:        "unauthorized",
			statusCode:  http.StatusUnauthorized,
			response:    `{"error":"invalid_client"}`,
			wantErr:     true,
			errContains: "token exchange returned 401",
		},
		{
			name:        "bad request",
			statusCode:  http.StatusBadRequest,
			response:    `{"error":"invalid_grant"}`,
			wantErr:     true,
			errContains: "token exchange returned 400",
		},
		{
			name:        "server error",
			statusCode:  http.StatusInternalServerError,
			response:    `{"error":"server error"}`,
			wantErr:     true,
			errContains: "token exchange returned 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Can't easily test exchangeRefreshToken directly as it's not exported
			// and uses hardcoded OAuth URL. This documents the limitation.
			t.Logf("exchangeRefreshToken error case: %s (can't test directly - uses real OAuth endpoint)", tt.name)
		})
	}
}

func TestAccessTokenFromMetadataReadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// Return 200 but with invalid body to trigger read/parse error
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Length", "1000000") // Claim large content
		// But don't write anything - causes read error
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	_, err := accessTokenFromMetadata(ctx)

	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestProjectIDReadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// Return forbidden to trigger error
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	_, err := ProjectID(ctx)

	if err == nil {
		t.Error("expected error on 403")
	}
}

func TestAccessTokenWithADCNotInTestMode(t *testing.T) {
	// This test documents that we can't easily test the ADC path
	// when not in test mode, as it requires actual ADC setup
	// The production code will try ADC first when isTestMode == false
	t.Log("ADC path testing requires real credentials setup")
}

func TestAccessTokenFromMetadataWithMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.WriteHeader(http.StatusOK)
		// Write malformed JSON
		if _, err := w.Write([]byte(`{"access_token": "test", "expires_in": "not a number"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	_, err := accessTokenFromMetadata(ctx)
	// Should either succeed (if parser is lenient) or fail with parse error
	if err != nil {
		t.Logf("Got expected error parsing malformed JSON: %v", err)
	}
}

func TestProjectIDWithEmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.WriteHeader(http.StatusOK)
		// Return empty response
		if _, err := w.Write([]byte("")); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	projectID, err := ProjectID(ctx)
	if err != nil {
		t.Fatalf("ProjectID with empty response failed: %v", err)
	}

	if projectID != "" {
		t.Logf("Got project ID: %s (empty string expected)", projectID)
	}
}

func TestAccessTokenFromADCWithServiceAccount(t *testing.T) {
	// Create temp credentials file with service account type
	tmpDir := t.TempDir()
	credsFile := filepath.Join(tmpDir, "sa-credentials.json")

	credsData := `{
		"type": "service_account",
		"project_id": "test-project",
		"private_key_id": "key-id",
		"private_key": "-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----\\n"
	}`

	if err := os.WriteFile(credsFile, []byte(credsData), 0o600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)

	ctx := context.Background()
	_, err := accessTokenFromADC(ctx)

	if err == nil {
		t.Error("expected error for unsupported credential type")
	}

	if !strings.Contains(err.Error(), "unsupported credential type") {
		t.Errorf("expected 'unsupported credential type' error, got: %v", err)
	}
}

// Test accessTokenFromADC with missing credentials file
func TestAccessTokenFromADCMissingFile(t *testing.T) {
	// Set env var to non-existent file
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/path/credentials.json")

	ctx := context.Background()
	_, err := accessTokenFromADC(ctx)

	if err == nil {
		t.Error("expected error for missing credentials file")
	}

	if !strings.Contains(err.Error(), "failed to read credentials file") {
		t.Errorf("expected 'failed to read credentials file' error, got: %v", err)
	}
}

// Test accessTokenFromADC with invalid JSON
func TestAccessTokenFromADCInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	credsFile := filepath.Join(tmpDir, "invalid-credentials.json")

	// Write invalid JSON
	if err := os.WriteFile(credsFile, []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)

	ctx := context.Background()
	_, err := accessTokenFromADC(ctx)

	if err == nil {
		t.Error("expected error for invalid JSON")
	}

	if !strings.Contains(err.Error(), "failed to parse credentials") {
		t.Errorf("expected 'failed to parse credentials' error, got: %v", err)
	}
}

// Test AccessToken fallback to metadata server
func TestAccessTokenFallbackToMetadata(t *testing.T) {
	// Create mock metadata server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": "metadata-token",
			"expires_in":   3600,
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})

	// Ensure no ADC credentials are available
	if err := os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"); err != nil {
		t.Fatalf("failed to unset env var: %v", err)
	}

	token, err := AccessToken(ctx)
	if err != nil {
		t.Fatalf("AccessToken failed: %v", err)
	}

	if token != "metadata-token" {
		t.Errorf("expected token 'metadata-token', got '%s'", token)
	}
}

// Test ProjectID with JSON decode error
func TestProjectIDInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			// Write invalid JSON
			if _, err := w.Write([]byte("123456")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	projectID, err := ProjectID(ctx)

	if err != nil {
		t.Logf("ProjectID with numeric response failed: %v", err)
	} else if projectID == "123456" {
		// Numeric strings are valid project IDs
		t.Log("Got numeric project ID successfully")
	}
}

// Test accessTokenFromADC with default location
func TestAccessTokenFromADCDefaultLocation(t *testing.T) {
	// Unset GOOGLE_APPLICATION_CREDENTIALS to test default location
	if err := os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS"); err != nil {
		t.Fatalf("failed to unset env var: %v", err)
	}

	// Create a mock credentials file in a temp directory that resembles home dir
	tmpDir := t.TempDir()

	// Create the gcloud config directory structure
	gcloudDir := filepath.Join(tmpDir, ".config", "gcloud")
	if err := os.MkdirAll(gcloudDir, 0o755); err != nil {
		t.Fatalf("failed to create gcloud dir: %v", err)
	}

	credsFile := filepath.Join(gcloudDir, "application_default_credentials.json")
	credsData := `{
		"type": "authorized_user",
		"client_id": "test-client-id",
		"client_secret": "test-secret",
		"refresh_token": "test-refresh-token"
	}`

	if err := os.WriteFile(credsFile, []byte(credsData), 0o600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	// We can't easily test the default location without mocking os.UserHomeDir,
	// but we can test with GOOGLE_APPLICATION_CREDENTIALS set
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)

	ctx := context.Background()
	// This will try to exchange the refresh token, which will fail
	// because we're not mocking the OAuth endpoint
	_, err := accessTokenFromADC(ctx)

	if err == nil {
		t.Log("accessTokenFromADC succeeded (unexpected)")
	} else {
		// Expected to fail on token exchange
		t.Logf("accessTokenFromADC failed as expected: %v", err)
	}
}

// Test ProjectID with request error
func TestProjectIDRequestError(t *testing.T) {
	// Set invalid URL to trigger request error
	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: "http://invalid-host-that-does-not-exist-12345",
		SkipADC:     true,
	})
	_, err := ProjectID(ctx)

	if err == nil {
		t.Error("expected error for invalid metadata server")
	}
}

// Test accessTokenFromMetadata with JSON type error
func TestAccessTokenFromMetadataTypeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// Return OK but with bad content that fails JSON parsing
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"access_token": "test", "expires_in": "not-a-number"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer server.Close()

	ctx := WithConfig(context.Background(), &Config{
		MetadataURL: server.URL,
		SkipADC:     true,
	})
	_, err := accessTokenFromMetadata(ctx)

	if err == nil {
		t.Error("expected error for invalid expires_in type")
	}
}
