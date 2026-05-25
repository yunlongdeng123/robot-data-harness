package config

import (
	"strings"
	"testing"
)

// secret 不能出现在 Redacted 输出中。
func TestRedactedDSN_HidesPassword(t *testing.T) {
	c := &Config{DBURI: "postgresql://app:supersecret@db.example:5432/robot?sslmode=disable"}
	out := c.RedactedDSN()
	if strings.Contains(out, "supersecret") {
		t.Fatalf("RedactedDSN leaked password: %s", out)
	}
	if !strings.Contains(out, "***") {
		t.Fatalf("RedactedDSN should contain *** placeholder: %s", out)
	}
}

func TestRedactedDSN_NoUserInfo(t *testing.T) {
	c := &Config{DBURI: "postgresql://db.example:5432/robot"}
	out := c.RedactedDSN()
	if strings.Contains(out, "***") {
		t.Fatalf("RedactedDSN should not introduce *** when no userinfo: %s", out)
	}
}
