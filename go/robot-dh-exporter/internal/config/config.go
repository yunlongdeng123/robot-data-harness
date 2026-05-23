package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 由环境变量解析得到。
//
// ROBOT_DH_DB_URI       PostgreSQL DSN（必填）
// EXPORTER_ADDR         监听地址，默认 :9108
// SCRAPE_INTERVAL_SEC   后台抓取周期，默认 30 秒
// LOG_LEVEL             info|debug|warn|error，默认 info
type Config struct {
	DBURI          string
	ListenAddr     string
	ScrapeInterval time.Duration
	LogLevel       string
}

// Load 从环境变量加载并校验配置。
//
// 出错时返回明确的错误，不会回显密码。
func Load() (*Config, error) {
	c := &Config{
		DBURI:          normalizeDBURI(strings.TrimSpace(os.Getenv("ROBOT_DH_DB_URI"))),
		ListenAddr:     envOr("EXPORTER_ADDR", ":9108"),
		ScrapeInterval: time.Duration(envIntOr("SCRAPE_INTERVAL_SEC", 30)) * time.Second,
		LogLevel:       strings.ToLower(envOr("LOG_LEVEL", "info")),
	}
	if c.DBURI == "" {
		return nil, errors.New("ROBOT_DH_DB_URI is required")
	}
	if c.ScrapeInterval < time.Second {
		return nil, fmt.Errorf("SCRAPE_INTERVAL_SEC must be >=1, got %s", c.ScrapeInterval)
	}
	return c, nil
}

func normalizeDBURI(dsn string) string {
	// Python/SQLAlchemy 使用的 driver 标记，pgx 只能解析标准 postgresql scheme。
	if strings.HasPrefix(dsn, "postgresql+psycopg://") {
		return "postgresql://" + strings.TrimPrefix(dsn, "postgresql+psycopg://")
	}
	return dsn
}

// RedactedDSN 返回隐去密码的 DSN，便于日志输出。
func (c *Config) RedactedDSN() string {
	return redactDSN(c.DBURI)
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			return n
		}
	}
	return def
}

// redactDSN 在 DSN 中把 password 字段替换成固定占位符。
//
// 仅支持常见 postgresql:// 格式；非 URL 格式直接返回 "***"。
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	const sep = "://"
	idx := strings.Index(dsn, sep)
	if idx < 0 {
		return "***"
	}
	rest := dsn[idx+len(sep):]
	at := strings.Index(rest, "@")
	if at < 0 {
		return dsn // 不含凭据
	}
	cred := rest[:at]
	tail := rest[at:]
	colon := strings.Index(cred, ":")
	if colon < 0 {
		return dsn
	}
	user := cred[:colon]
	return dsn[:idx+len(sep)] + user + ":***" + tail
}
