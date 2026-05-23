package config

import (
	"os"
	"strings"
	"testing"
)

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"basic", "postgresql://user:pw@host:5432/db", "postgresql://user:***@host:5432/db"},
		{"no_creds", "postgresql://host:5432/db", "postgresql://host:5432/db"},
		{"empty", "", ""},
		{"not_url", "abc=1 password=pw", "***"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactDSN(tt.in)
			if got != tt.want {
				t.Fatalf("redactDSN(%q)=%q want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestLoadMissingDSN(t *testing.T) {
	t.Setenv("ROBOT_DH_DB_URI", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "ROBOT_DH_DB_URI") {
		t.Fatalf("expected ROBOT_DH_DB_URI error, got %v", err)
	}
}

func TestLoadDefaults(t *testing.T) {
	os.Setenv("ROBOT_DH_DB_URI", "postgresql://u:p@h/db")
	defer os.Unsetenv("ROBOT_DH_DB_URI")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if cfg.ListenAddr != ":9108" {
		t.Fatalf("default addr wrong: %s", cfg.ListenAddr)
	}
	if cfg.ScrapeInterval.Seconds() != 30 {
		t.Fatalf("default interval wrong: %s", cfg.ScrapeInterval)
	}
	if cfg.RedactedDSN() != "postgresql://u:***@h/db" {
		t.Fatalf("redacted wrong: %s", cfg.RedactedDSN())
	}
}

func TestLoadNormalizesSQLAlchemyPostgresDSN(t *testing.T) {
	os.Setenv("ROBOT_DH_DB_URI", "postgresql+psycopg://u:p@h/db")
	defer os.Unsetenv("ROBOT_DH_DB_URI")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if cfg.DBURI != "postgresql://u:p@h/db" {
		t.Fatalf("DBURI=%q", cfg.DBURI)
	}
	if cfg.RedactedDSN() != "postgresql://u:***@h/db" {
		t.Fatalf("redacted wrong: %s", cfg.RedactedDSN())
	}
}
