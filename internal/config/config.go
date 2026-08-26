package config

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"stellarbill-backend/internal/secrets"
	"strconv"
	"strings"
	"unicode"
)

// ConfigErrorType represents the category of configuration error
type ConfigErrorType string

const (
	ErrMissingEnvVar    ConfigErrorType = "MISSING_ENV_VAR"
	ErrInvalidPort      ConfigErrorType = "INVALID_PORT"
	ErrInvalidURL       ConfigErrorType = "INVALID_URL"
	ErrWeakSecret       ConfigErrorType = "WEAK_SECRET"
	ErrInvalidValue     ConfigErrorType = "INVALID_VALUE"
	ErrValidationFailed ConfigErrorType = "VALIDATION_FAILED"
)

// ConfigError represents a typed configuration error
type ConfigError struct {
	Type    ConfigErrorType
	Key     string
	Message string
	Value   string
}

func (e *ConfigError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("config error [%s]: %s (key=%s, value=%s)", e.Type, e.Message, e.Key, e.Value)
	}
	return fmt.Sprintf("config error [%s]: %s", e.Type, e.Message)
}

// Config holds all application configuration
type Config struct {
	Env                    string   `json:"env"`
	Port                   int      `json:"port"`
	DBConn                 string   `json:"db_conn" secret:"true"`
	JWTSecret              string   `json:"jwt_secret" secret:"true"`
	MaxHeaderBytes         int      `json:"max_header_bytes"`
	ReadTimeout            int      `json:"read_timeout"`
	WriteTimeout           int      `json:"write_timeout"`
	IdleTimeout            int      `json:"idle_timeout"`
	AllowedOrigins         string   `json:"allowed_origins"`
	AdminToken             string   `json:"admin_token" secret:"true"`
	DBReplicaConn          string   `json:"db_replica_conn" secret:"true"`
	// Rate limiting configuration
	RateLimitEnabled   bool     `json:"rate_limit_enabled"`
	RateLimitMode      string   `json:"rate_limit_mode"`
	RateLimitRPS       int      `json:"rate_limit_rps"`
	RateLimitBurst     int      `json:"rate_limit_burst"`
	RateLimitWhitelist []string `json:"rate_limit_whitelist"`
	// Tracing configuration
	TracingExporter        string
	TracingServiceName     string
	SecurityFrameAncestors string
	// SecurityCSPReportURI is the endpoint browsers will POST CSP violation
	// reports to. Set to the /api/v1/csp-reports sink. Leave empty to omit
	// the report-uri directive (no collection).
	SecurityCSPReportURI string
	// CSPReportRPS is the per-tenant sustained rate (requests per second)
	// allowed on the /api/v1/csp-reports endpoint. Default: 5.
	CSPReportRPS int
	// CSPReportBurst is the per-tenant burst size for /api/v1/csp-reports.
	// Default: 10.
	CSPReportBurst int
	SpiffeSocketPath   string
	SpiffeTrustDomain  string
	MaxRequestSize     int64
	MaxGzipUncompressed int64
	MaxGzipRatio       float64
	// RedisURL configures the Redis cache backend. When empty, an in-memory
	// cache is used instead.
	RedisURL string `json:"redis_url" secret:"true"`
	CacheTTL int    `json:"cache_ttl"`

	// DB connection pool tuning.
	// All durations are in seconds to keep env-var parsing uniform.
	//
	//   DB_POOL_MAX_CONNS            (default 25)  – hard ceiling on open connections.
	//   DB_POOL_MIN_CONNS            (default 2)   – connections kept warm at all times.
	//   DB_POOL_MAX_CONN_LIFETIME    (default 3600) – recycle connections after this many
	//                                                 seconds to spread load across replicas
	//                                                 and avoid stale TCP sessions.
	//   DB_POOL_MAX_CONN_IDLE_TIME   (default 600)  – evict idle connections after this
	//                                                 many seconds; prevents firewall drops.
	//   DB_POOL_CONNECT_TIMEOUT      (default 5)   – per-dial timeout in seconds.
	//   DB_POOL_HEALTH_CHECK_PERIOD  (default 30)  – how often pgxpool probes idle conns.
	//   DB_POOL_METRICS_INTERVAL     (default 15)  – how often pool stats are scraped
	//                                                 into Prometheus gauges.
	DBPoolMaxConns          int `json:"db_pool_max_conns"`
	DBPoolMinConns          int `json:"db_pool_min_conns"`
	DBPoolMaxConnLifetime   int `json:"db_pool_max_conn_lifetime"`
	DBPoolMaxConnIdleTime   int `json:"db_pool_max_conn_idle_time"`
	DBPoolConnectTimeout    int `json:"db_pool_connect_timeout"`
	DBPoolHealthCheckPeriod int `json:"db_pool_health_check_period"`
	DBPoolMetricsInterval   int `json:"db_pool_metrics_interval"`

	// PgBouncer sidecar configuration.
	PgBouncerEnabled         bool
	PgBouncerHost            string
	PgBouncerPort            int
	DBStatementCacheMode     string
	PgBouncerIdleInTxTimeout int
	GracefulShutdownTimeout  int
	ConcurrencyCapsPath      string
	OTelLogsEnabled          bool

	// Outbox sharding configuration.
	OutboxShardCount  int   `json:"outbox_shard_count"`
	OutboxOwnedShards []int `json:"outbox_owned_shards"`
}

// ValidationResult holds the result of configuration validation
type ValidationResult struct {
	Errors   []ConfigError
	Warnings []string
}

// Valid returns true if there are no validation errors
func (v *ValidationResult) Valid() bool {
	return len(v.Errors) == 0
}

// Error returns a formatted string of all validation errors
func (v *ValidationResult) Error() string {
	if v.Valid() {
		return ""
	}
	var errs []string
	for _, e := range v.Errors {
		errs = append(errs, e.Error())
	}
	return strings.Join(errs, "; ")
}

// Constants for configuration limits
const (
	DefaultPort         = 8080
	MinPort             = 1
	MaxPort             = 65535
	MinSecretLength     = 12
	MaxHeaderBytes      = 1 << 20 // 1MB
	DefaultReadTimeout  = 30      // seconds
	DefaultWriteTimeout = 30      // seconds
	DefaultIdleTimeout  = 120     // seconds

	DefaultDBPoolMaxConns          = 25
	DefaultDBPoolMinConns          = 2
	DefaultDBPoolMaxConnLifetime   = 3600
	DefaultDBPoolMaxConnIdleTime   = 600
	DefaultDBPoolConnectTimeout    = 5
	DefaultDBPoolHealthCheckPeriod = 30
	DefaultDBPoolMetricsInterval   = 15

	DefaultGracefulShutdownTimeout = 30
	MinDBPoolMaxConns              = 1
	MaxDBPoolMaxConns              = 500
	MinDBPoolTimeout               = 1
	MaxDBPoolTimeout               = 300

	DefaultPgBouncerHost            = "127.0.0.1"
	DefaultPgBouncerPort            = 5432
	DefaultDBStatementCacheMode     = "prepare"
	DefaultPgBouncerIdleInTxTimeout = 30
	MinPgBouncerPort                = 1
	MaxPgBouncerPort                = 65535

	StatementCacheModeDescribe = "describe"
	StatementCacheModePrepare  = "prepare"
	StatementCacheModeSimple   = "simple"

	MinHeaderBytes        = 1024
	MaxAllowedHeaderBytes = 10 << 20
	MinTimeoutSeconds     = 1
	MaxTimeoutSeconds     = 600
	MinRateLimitRPS       = 1
	MaxRateLimitRPS       = 1000
	MinRateLimitBurst     = 1
	MaxRateLimitBurst     = 2000
)

// Option configures the Load function.
type Option func(*loadOptions)

type loadOptions struct {
	secretsProvider secrets.Provider
}

// WithSecretsProvider overrides the default env-based secrets provider.
func WithSecretsProvider(p secrets.Provider) Option {
	return func(o *loadOptions) {
		o.secretsProvider = p
	}
}

var secretKeys = []string{
	"DATABASE_URL",
	"JWT_SECRET",
	"ADMIN_TOKEN",
	"REDIS_URL",
}

func Load(opts ...Option) (Config, error) {
	o := &loadOptions{
		secretsProvider: secrets.NewEnvProvider(),
	}
	for _, fn := range opts {
		fn(o)
	}

	cfg := Config{
		Env:                    getEnv("ENV", "development"),
		Port:                   DefaultPort,
		DBConn:                 "",
		JWTSecret:              "",
		MaxHeaderBytes:         MaxHeaderBytes,
		ReadTimeout:            DefaultReadTimeout,
		WriteTimeout:           DefaultWriteTimeout,
		IdleTimeout:            DefaultIdleTimeout,
		TracingExporter:        getEnv("TRACING_EXPORTER", "stdout"),
		TracingServiceName:     getEnv("TRACING_SERVICE_NAME", "stellabill-backend"),
		OTelLogsEnabled:        getEnvBool("OTEL_LOGS_ENABLED", false),
		SecurityFrameAncestors: getEnv("SECURITY_FRAME_ANCESTORS", "'none'"),
		SecurityCSPReportURI:   getEnv("SECURITY_CSP_REPORT_URI", "/api/v1/csp-reports"),
		CSPReportRPS:           getEnvInt("CSP_REPORT_RPS", 5),
		CSPReportBurst:         getEnvInt("CSP_REPORT_BURST", 10),
		MaxRequestSize:         getEnvInt64("MAX_REQUEST_SIZE", 1024*1024*10),
		MaxGzipUncompressed:    getEnvInt64("MAX_GZIP_UNCOMPRESSED", 1024*1024*50),
		MaxGzipRatio:           getEnvFloat64("MAX_GZIP_RATIO", 10.0),
		DBReplicaConn:          getEnv("DB_REPLICA_URL", ""),
		RedisURL:               getEnv("REDIS_URL", ""),
		CacheTTL:               getEnvInt("CACHE_TTL", 60),
		DBPoolMaxConns:         DefaultDBPoolMaxConns,
		DBPoolMinConns:         DefaultDBPoolMinConns,
		DBPoolMaxConnLifetime:  DefaultDBPoolMaxConnLifetime,
		DBPoolMaxConnIdleTime:  DefaultDBPoolMaxConnIdleTime,
		DBPoolConnectTimeout:   DefaultDBPoolConnectTimeout,
		DBPoolHealthCheckPeriod: DefaultDBPoolHealthCheckPeriod,
		DBPoolMetricsInterval:  DefaultDBPoolMetricsInterval,
		PgBouncerEnabled:       false,
		PgBouncerHost:          DefaultPgBouncerHost,
		PgBouncerPort:          DefaultPgBouncerPort,
		DBStatementCacheMode:   DefaultDBStatementCacheMode,
		PgBouncerIdleInTxTimeout: DefaultPgBouncerIdleInTxTimeout,
		GracefulShutdownTimeout:  DefaultGracefulShutdownTimeout,
		OutboxShardCount:         getEnvInt("OUTBOX_SHARD_COUNT", 0),
		OutboxOwnedShards:        parseShards(getEnv("OUTBOX_OWNED_SHARDS", "")),
	}

	// Secrets from the provider
	dbURL, err := o.secretsProvider.Get("DATABASE_URL")
	if err != nil {
		return Config{}, &ConfigError{Type: ErrMissingEnvVar, Key: "DATABASE_URL", Message: "missing database url"}
	}
	cfg.DBConn = dbURL

	jwtSecret, err := o.secretsProvider.Get("JWT_SECRET")
	if err != nil {
		return Config{}, &ConfigError{Type: ErrMissingEnvVar, Key: "JWT_SECRET", Message: "missing jwt secret"}
	}
	cfg.JWTSecret = jwtSecret

	adminToken, err := o.secretsProvider.Get("ADMIN_TOKEN")
	if err != nil {
		return Config{}, &ConfigError{Type: ErrMissingEnvVar, Key: "ADMIN_TOKEN", Message: "missing admin token"}
	}
	cfg.AdminToken = adminToken

	// Validate the configuration
	vr := cfg.Validate()
	if !vr.Valid() {
		return Config{}, errors.New(vr.Error())
	}

	return cfg, nil
}

func parseShards(s string) []int {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	shards := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			continue // ignore invalid entries
		}
		shards = append(shards, v)
	}
	return shards
}

// getEnv returns the environment variable value or a default.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			return i
		}
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return i
		}
	}
	return def
}

func getEnvFloat64(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return f
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}
