// Package cmd implements the offsetpeek CLI.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
	"github.com/zahidhasann88/offsetpeek/internal/kafkax"
	"github.com/zahidhasann88/offsetpeek/internal/preview"
	"github.com/zahidhasann88/offsetpeek/internal/render"
	"github.com/zahidhasann88/offsetpeek/internal/reset"
)

// Executor builds the AdminClient; tests replace it with a fake.
var Executor = func(ctx context.Context, cfg kafkax.Config) (admin.AdminClient, *CloseFn, error) {
	cl, _, err := kafkax.NewClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	return kafkax.Wrap(cl), &CloseFn{cl: cl}, nil
}

// now is the clock for relative --to-time values; tests override it.
var now = time.Now

type CloseFn struct {
	cl interface{ Close() }
}

func (c *CloseFn) Close() error {
	if c.cl != nil {
		c.cl.Close()
	}
	return nil
}

// ExitError carries a specific process exit code.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

// ExitActiveMembers is the exit code for --fail-on-active.
const ExitActiveMembers = 2

func RootCmd() *cobra.Command {
	flags := &cliFlags{}

	cmd := &cobra.Command{
		Use:   "offsetpeek",
		Short: "Preview a Kafka consumer-group offset reset without changing anything.",
		Long: `offsetpeek is a read-only preview tool for Kafka consumer-group offset resets.

For every topic-partition the reset would touch it prints the committed
offset, the target offset, the delta and a verdict (REPROCESS / SKIP /
NO CHANGE / NEW / UNRESOLVED), plus totals and the equivalent
kafka-consumer-groups.sh command (printed only, never executed).

offsetpeek never calls a write API.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.Flags().SortFlags = false
	registerFlags(cmd.Flags(), flags)

	cmd.PreRunE = func(c *cobra.Command, _ []string) error {
		return flags.applyEnvAndValidate()
	}
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		return run(c.Context(), c.OutOrStdout(), c.ErrOrStderr(), flags)
	}
	return cmd
}

type cliFlags struct {
	brokers       string
	mechanism     string
	user          string
	password      string
	tlsEnable     bool
	tlsSkipVerify bool
	tlsCAFile     string
	tlsCertFile   string
	tlsKeyFile    string

	group string
	topic string
	json  bool

	toTimeStr  string
	toEarliest bool
	toLatest   bool
	shiftBy    int64
	toOffset   []string
	fromFile   string

	bytesPerMessage int64
	sampleRecords   int
	commandConfig   string
	timeout         time.Duration
	failOnActive    bool
}

func registerFlags(fs *pflag.FlagSet, f *cliFlags) {
	fs.StringVar(&f.brokers, "brokers", "", "Comma-separated broker list (host:port[,host:port]). Env: BROKERS")
	fs.StringVar(&f.mechanism, "sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Env: SASL_MECHANISM")
	fs.StringVar(&f.user, "sasl-user", "", "SASL user. Env: SASL_USER (or KAFKA_SASL_USER)")
	fs.StringVar(&f.password, "sasl-password", "", "SASL password. Env: SASL_PASSWORD (or KAFKA_SASL_PASSWORD)")
	fs.BoolVar(&f.tlsEnable, "tls", false, "Enable TLS. Env: KAFKA_TLS")
	fs.BoolVar(&f.tlsSkipVerify, "tls-skip-verify", false, "Skip TLS certificate verification (not recommended). Env: KAFKA_TLS_SKIP_VERIFY")
	fs.StringVar(&f.tlsCAFile, "tls-ca-file", "", "PEM CA certificate file. Env: KAFKA_TLS_CA_FILE")
	fs.StringVar(&f.tlsCertFile, "tls-cert-file", "", "PEM client certificate file. Env: KAFKA_TLS_CERT_FILE")
	fs.StringVar(&f.tlsKeyFile, "tls-key-file", "", "Client certificate key file. Env: KAFKA_TLS_KEY_FILE")

	fs.StringVar(&f.group, "group", "", "Consumer group. Env: GROUP")
	fs.StringVar(&f.topic, "topic", "", "Limit the reset to one topic. Env: TOPIC")
	fs.BoolVar(&f.json, "json", false, "Emit JSON instead of a table.")

	fs.StringVar(&f.toTimeStr, "to-time", "",
		"Reset to a time: RFC3339 (2026-10-13T10:00:00+06:00) or relative to now (-2h, -90m).")
	fs.BoolVar(&f.toEarliest, "to-earliest", false, "Reset to the earliest offset.")
	fs.BoolVar(&f.toLatest, "to-latest", false, "Reset to the latest offset.")
	fs.Int64Var(&f.shiftBy, "shift-by", 0, "Shift the committed offset by N messages (negative = reprocess).")
	fs.StringArrayVar(&f.toOffset, "to-offset", nil,
		"Partition offset: topic:partition=offset, or partition=offset with --topic. Repeatable.")
	fs.StringVar(&f.fromFile, "from-file", "",
		"CSV of topic,partition,offset lines (kafka-consumer-groups.sh --from-file format).")

	fs.IntVar(&f.sampleRecords, "sample", 0, "Fetch up to N records per affected partition to estimate bytes.")
	fs.Int64Var(&f.bytesPerMessage, "estimate-bytes-avg", 0, "Fixed bytes/message estimate (fallback when not sampled).")
	fs.StringVar(&f.commandConfig, "command-config", "",
		"client.properties path for the printed command (placeholder used with SASL/TLS).")
	fs.DurationVar(&f.timeout, "timeout", 30*time.Second, "Overall time limit for talking to Kafka.")
	fs.BoolVar(&f.failOnActive, "fail-on-active", false,
		fmt.Sprintf("Exit with code %d if the group has active members.", ExitActiveMembers))
}

func (f *cliFlags) applyEnvAndValidate() error {
	envDefault(&f.brokers, "BROKERS")
	envDefault(&f.user, "SASL_USER", "KAFKA_SASL_USER")
	envDefault(&f.password, "SASL_PASSWORD", "KAFKA_SASL_PASSWORD")
	envDefault(&f.mechanism, "SASL_MECHANISM")
	envDefault(&f.tlsCAFile, "KAFKA_TLS_CA_FILE")
	envDefault(&f.tlsCertFile, "KAFKA_TLS_CERT_FILE")
	envDefault(&f.tlsKeyFile, "KAFKA_TLS_KEY_FILE")
	envDefault(&f.group, "GROUP")
	envDefault(&f.topic, "TOPIC")
	f.tlsEnable = f.tlsEnable || envBool("KAFKA_TLS")
	f.tlsSkipVerify = f.tlsSkipVerify || envBool("KAFKA_TLS_SKIP_VERIFY")

	if f.brokers == "" {
		return errors.New("--brokers (or $BROKERS) is required")
	}
	if f.group == "" {
		return errors.New("--group (or $GROUP) is required")
	}
	if f.sampleRecords < 0 || f.bytesPerMessage < 0 {
		return errors.New("--sample and --estimate-bytes-avg must be >= 0")
	}
	if f.timeout <= 0 {
		return errors.New("--timeout must be > 0")
	}
	if f.tlsSkipVerify {
		f.tlsEnable = true
	}
	if len(f.toOffset) > 0 && f.fromFile != "" {
		return errors.New("use either --to-offset or --from-file, not both")
	}

	targets := 0
	for _, set := range []bool{f.toTimeStr != "", f.toEarliest, f.toLatest, f.shiftBy != 0, len(f.toOffset) > 0 || f.fromFile != ""} {
		if set {
			targets++
		}
	}
	if targets != 1 {
		return errors.New("exactly one of --to-time / --to-earliest / --to-latest / --shift-by / --to-offset / --from-file is required")
	}
	return nil
}

func envDefault(dst *string, keys ...string) {
	for _, k := range keys {
		if *dst != "" {
			return
		}
		*dst = os.Getenv(k)
	}
}

func envBool(key string) bool {
	v := os.Getenv(key)
	return v == "1" || strings.EqualFold(v, "true")
}

func run(ctx context.Context, stdout, stderr io.Writer, flags *cliFlags) error {
	ctx, cancel := context.WithTimeout(ctx, flags.timeout)
	defer cancel()

	target, err := resolveTarget(flags)
	if err != nil {
		return err
	}

	cfg := kafkax.Config{
		Brokers:       splitBrokers(flags.brokers),
		Mechanism:     flags.mechanism,
		User:          flags.user,
		Password:      flags.password,
		TLSSkipVerify: flags.tlsSkipVerify,
		TLSEnable:     flags.tlsEnable,
		TLSCAFile:     flags.tlsCAFile,
		TLSCertFile:   flags.tlsCertFile,
		TLSKeyFile:    flags.tlsKeyFile,
	}

	a, closer, err := Executor(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if closer != nil {
		defer func() {
			if e := closer.Close(); e != nil {
				fmt.Fprintln(stderr, "warning: client close:", e)
			}
			a.Close()
		}()
	}

	opts := preview.Options{
		Brokers:         flags.brokers,
		Group:           flags.group,
		Topic:           flags.topic,
		Target:          target,
		BytesPerMessage: flags.bytesPerMessage,
		SampleRecords:   flags.sampleRecords,
		CommandConfig:   flags.commandConfig,
		OffsetsFile:     flags.fromFile,
	}
	if opts.CommandConfig == "" && (cfg.Mechanism != "" || cfg.TLSEnable) {
		opts.CommandConfig = "<client.properties>"
	}
	report, err := preview.Run(ctx, a, opts)
	if err != nil {
		return err
	}

	if flags.json {
		err = render.JSON(report, stdout)
	} else {
		err = render.Table(report, stdout)
	}
	if err != nil {
		return err
	}
	if flags.failOnActive && report.ActiveMembers != nil {
		return &ExitError{
			Code: ExitActiveMembers,
			Err:  fmt.Errorf("group %q has %d active member(s) (--fail-on-active)", flags.group, report.ActiveMembers.MemberCount),
		}
	}
	return nil
}

func resolveTarget(flags *cliFlags) (reset.Target, error) {
	switch {
	case flags.toTimeStr != "":
		t, err := parseTime(flags.toTimeStr)
		if err != nil {
			return reset.Target{}, err
		}
		return reset.Target{Kind: reset.KindTime, Value: t}, nil
	case flags.toEarliest:
		return reset.Target{Kind: reset.KindEarliest}, nil
	case flags.toLatest:
		return reset.Target{Kind: reset.KindLatest}, nil
	case flags.shiftBy != 0:
		return reset.Target{Kind: reset.KindShift, Shift: flags.shiftBy}, nil
	case flags.fromFile != "":
		f, err := os.Open(flags.fromFile)
		if err != nil {
			return reset.Target{}, fmt.Errorf("--from-file: %w", err)
		}
		defer f.Close()
		pos, err := reset.ParseOffsetsCSV(f)
		if err != nil {
			return reset.Target{}, fmt.Errorf("--from-file %s: %w", flags.fromFile, err)
		}
		return reset.Target{Kind: reset.KindOffset, Offsets: pos}, nil
	case len(flags.toOffset) > 0:
		var pos []reset.PartitionOffset
		for _, spec := range flags.toOffset {
			po, err := reset.ParseOffsetSpec(spec)
			if err != nil {
				return reset.Target{}, err
			}
			if po.Topic == "" {
				if flags.topic == "" {
					return reset.Target{}, errors.New("--to-offset partition=offset needs --topic (or use topic:partition=offset)")
				}
				po.Topic = flags.topic
			}
			pos = append(pos, *po)
		}
		return reset.Target{Kind: reset.KindOffset, Offsets: pos}, nil
	}
	return reset.Target{}, errors.New("no target specified")
}

// parseTime accepts RFC3339 or a Go duration relative to now ("-2h").
func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--to-time %q: want RFC3339 (2026-10-13T10:00:00+06:00) or a duration like -2h", s)
	}
	return now().Add(d), nil
}

func splitBrokers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
