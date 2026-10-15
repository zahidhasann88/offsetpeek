// Package kafkax builds franz-go clients from connection settings.
package kafkax

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/example/offsetpeek/internal/admin"
)

type Config struct {
	Brokers []string

	// Mechanism: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512; empty disables SASL.
	Mechanism string
	User      string
	Password  string

	TLSSkipVerify bool
	TLSCAFile     string
	TLSKeyFile    string
	TLSCertFile   string
	TLSEnable     bool
}

// NewClient builds the kgo and kadm clients. The caller closes the kgo client.
func NewClient(cfg Config) (*kgo.Client, *kadm.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, nil, errors.New("at least one broker is required")
	}

	opts := []kgo.Opt{
		kgo.ClientID("offsetpeek"),
		kgo.SeedBrokers(cfg.Brokers...),
	}

	if cfg.Mechanism != "" {
		mech := strings.ToUpper(strings.TrimSpace(cfg.Mechanism))
		switch mech {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: cfg.User,
				Pass: cfg.Password,
			}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.User,
				Pass: cfg.Password,
			}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cfg.User,
				Pass: cfg.Password,
			}.AsSha512Mechanism()))
		default:
			return nil, nil, fmt.Errorf("unknown SASL mechanism %q (want PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.Mechanism)
		}
	}

	if cfg.TLSEnable || cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" || cfg.TLSCAFile != "" || cfg.TLSSkipVerify {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec // opt-in
		}
		if cfg.TLSCAFile != "" {
			pem, err := os.ReadFile(cfg.TLSCAFile)
			if err != nil {
				return nil, nil, fmt.Errorf("read CA file %q: %w", cfg.TLSCAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, nil, fmt.Errorf("CA file %q contained no certificates", cfg.TLSCAFile)
			}
			tlsCfg.RootCAs = pool
		}
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				return nil, nil, fmt.Errorf("load client cert/key: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return cl, kadm.NewClient(cl), nil
}

// Wrap returns an AdminClient for cl with record sampling enabled.
func Wrap(cl *kgo.Client) admin.AdminClient {
	return admin.NewKadmClient(kadm.NewClient(cl)).WithSampling(cl)
}
