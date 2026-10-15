package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zahidhasann88/offsetpeek/internal/admin"
	"github.com/zahidhasann88/offsetpeek/internal/kafkax"
)

func withExecutor(t *testing.T, fn executorFunc) {
	t.Helper()
	prev := Executor
	Executor = fn
	t.Cleanup(func() { Executor = prev })
}

type executorFunc func(ctx context.Context, cfg kafkax.Config) (admin.AdminClient, *CloseFn, error)

func executorWith(t *testing.T, fake admin.AdminClient) executorFunc {
	return func(ctx context.Context, cfg kafkax.Config) (admin.AdminClient, *CloseFn, error) {
		return fake, &CloseFn{}, nil
	}
}

func runCmd(t *testing.T, args ...string) (string, string, error) {
	t.Helper()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	c := RootCmd()
	c.SetOut(stdout)
	c.SetErr(stderr)
	c.SetArgs(args)

	err := c.Execute()
	return stdout.String(), stderr.String(), err
}

func TestCLIFlags_MinimalArgs(t *testing.T) {
	out, _, err := runCmd(t, "--help")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "offsetpeek") {
		t.Fatalf("help didn't mention tool name: %q", out)
	}
	if !strings.Contains(out, "--group") {
		t.Fatalf("help didn't mention --group: %q", out)
	}
}

func TestCLIFlags_MissingArgs(t *testing.T) {
	_, _, err := runCmd(t)
	if err == nil {
		t.Fatal("expected error for no args")
	}
	if !strings.Contains(err.Error(), "brokers") && !strings.Contains(err.Error(), "group") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLIFlags_MultipleTargets(t *testing.T) {
	_, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-earliest", "--to-latest")
	if err == nil {
		t.Fatal("expected error for two targets")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLIFlags_BadTime(t *testing.T) {
	_, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-time", "not-a-time")
	if err == nil {
		t.Fatal("expected bad-time error")
	}
	if !strings.Contains(err.Error(), "--to-time") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLI_FullRunWithFakeAdmin(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 1000},
		},
	}
	withExecutor(t, executorWith(t, fake))

	out, _, err := runCmd(t,
		"--brokers", "x:9092",
		"--group", "payments",
		"--topic", "orders",
		"--to-earliest",
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out, "REPROCESS") {
		t.Fatalf("missing REPROCESS in table output:\n%s", out)
	}
	if !strings.Contains(out, "kafka-consumer-groups.sh") {
		t.Fatalf("missing reset command in output:\n%s", out)
	}
	if !strings.Contains(out, "--to-earliest") {
		t.Fatalf("missing --to-earliest in reset command:\n%s", out)
	}
}

func TestCLI_JSONOutput(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "orders", Partition: 0, Committed: 100},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 1000},
		},
	}
	withExecutor(t, executorWith(t, fake))
	out, _, err := runCmd(t,
		"--brokers", "x:9092",
		"--group", "payments",
		"--topic", "orders",
		"--to-earliest",
		"--json",
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out, `"group": "payments"`) {
		t.Fatalf("missing JSON field group:\n%s", out)
	}
	if !strings.Contains(out, `"was_dry_run": true`) {
		t.Fatalf("missing JSON field was_dry_run:\n%s", out)
	}
	if !strings.Contains(out, `"kind": "to-earliest"`) {
		t.Fatalf("target kind should serialize as a name:\n%s", out)
	}
}

func TestCLI_EnvBrokers(t *testing.T) {
	t.Setenv("BROKERS", "env-broker:9092")
	t.Setenv("GROUP", "env-group")
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{
			{Topic: "t", Partition: 0, Committed: 50},
		},
		Ranges: []admin.PartitionRange{
			{Topic: "t", Partition: 0, Earliest: 0, Latest: 1000},
		},
	}
	withExecutor(t, executorWith(t, fake))
	out, _, err := runCmd(t, "--to-earliest") // brokers & group via env
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out, "env-broker:9092") {
		t.Fatalf("env BROKERS not honored: %s", out)
	}
	if !strings.Contains(out, "env-group") {
		t.Fatalf("env GROUP not honored: %s", out)
	}
}

func TestCLI_ConnectErrorBubblesUp(t *testing.T) {
	withExecutor(t, func(_ context.Context, _ kafkax.Config) (admin.AdminClient, *CloseFn, error) {
		return nil, nil, errors.New("connection refused")
	})
	_, _, err := runCmd(t,
		"--brokers", "x:9092",
		"--group", "g",
		"--to-earliest",
	)
	if err == nil {
		t.Fatal("expected connect error to propagate")
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCLI_NoOutputOnStderr(t *testing.T) {
	fake := &admin.FakeAdminClient{}
	withExecutor(t, executorWith(t, fake))
	_, stderr, err := runCmd(t,
		"--brokers", "x:9092",
		"--group", "g",
		"--to-earliest",
	)
	if err != nil {
		t.Fatal(err)
	}
	if stderr != "" {
		t.Fatalf("unexpected stderr: %q", stderr)
	}
}

func TestCLI_ToOffsetUsesTopicFlag(t *testing.T) {
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{{Topic: "orders", Partition: 0, Committed: 10}},
		Ranges:  []admin.PartitionRange{{Topic: "orders", Partition: 0, Earliest: 0, Latest: 20}},
	}
	withExecutor(t, executorWith(t, fake))
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--topic", "orders", "--to-offset", "0=5")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "REPROCESS 5") || !strings.Contains(out, "--topic orders:0 --to-offset 5") {
		t.Fatalf("output:\n%s", out)
	}
}

func TestCLI_ToOffsetWithoutTopic(t *testing.T) {
	_, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-offset", "0=5")
	if err == nil || !strings.Contains(err.Error(), "--topic") {
		t.Fatalf("want --topic error, got %v", err)
	}
}

func TestCLI_SASLAddsCommandConfig(t *testing.T) {
	var gotCfg kafkax.Config
	withExecutor(t, func(_ context.Context, cfg kafkax.Config) (admin.AdminClient, *CloseFn, error) {
		gotCfg = cfg
		return &admin.FakeAdminClient{
			Offsets: []admin.PartitionProgress{{Topic: "t", Partition: 0, Committed: 1}},
			Ranges:  []admin.PartitionRange{{Topic: "t", Partition: 0, Earliest: 0, Latest: 2}},
		}, &CloseFn{}, nil
	})
	t.Setenv("SASL_PASSWORD", "from-env")
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-earliest",
		"--sasl-mechanism", "SCRAM-SHA-512", "--sasl-user", "alice", "--tls")
	if err != nil {
		t.Fatal(err)
	}
	if gotCfg.Password != "from-env" || gotCfg.User != "alice" || !gotCfg.TLSEnable {
		t.Fatalf("config: %+v", gotCfg)
	}
	if !strings.Contains(out, "--command-config '<client.properties>'") {
		t.Fatalf("output:\n%s", out)
	}
}

func liveFake() *admin.FakeAdminClient {
	return &admin.FakeAdminClient{
		Offsets:     []admin.PartitionProgress{{Topic: "orders", Partition: 0, Committed: 10}},
		Ranges:      []admin.PartitionRange{{Topic: "orders", Partition: 0, Earliest: 0, Latest: 20}},
		GroupResult: admin.GroupDescription{State: "Stable", Members: []admin.MemberInfo{{MemberID: "m1"}}},
	}
}

func TestCLI_FailOnActive(t *testing.T) {
	withExecutor(t, executorWith(t, liveFake()))
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-earliest", "--fail-on-active")
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != ExitActiveMembers {
		t.Fatalf("want ExitError code %d, got %v", ExitActiveMembers, err)
	}
	if !strings.Contains(out, "ACTIVE member") {
		t.Fatalf("report should still be printed:\n%s", out)
	}

	withExecutor(t, executorWith(t, liveFake()))
	if _, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-earliest"); err != nil {
		t.Fatalf("without the flag active members are only a warning: %v", err)
	}
}

func TestCLI_RelativeTime(t *testing.T) {
	fixed := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	prev := now
	now = func() time.Time { return fixed }
	t.Cleanup(func() { now = prev })

	withExecutor(t, executorWith(t, &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{{Topic: "orders", Partition: 0, Committed: 10}},
		Ranges:  []admin.PartitionRange{{Topic: "orders", Partition: 0, Earliest: 0, Latest: 20}},
	}))
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--to-time", "-2h")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "--to-datetime 2026-10-13T10:00:00.000Z") {
		t.Fatalf("output:\n%s", out)
	}
}

func TestCLI_MultipleToOffsets(t *testing.T) {
	withExecutor(t, executorWith(t, &admin.FakeAdminClient{
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 20},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 20},
		},
	}))
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--topic", "orders",
		"--to-offset", "0=5", "--to-offset", "orders:1=7")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--from-file offsets.csv --execute", "with offsets.csv:\norders,0,5\norders,1,7"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestCLI_FromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.csv")
	if err := os.WriteFile(path, []byte("orders,0,5\norders,1,7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &admin.FakeAdminClient{
		Offsets: []admin.PartitionProgress{{Topic: "orders", Partition: 0, Committed: 10}, {Topic: "orders", Partition: 1, Committed: 3}},
		Ranges: []admin.PartitionRange{
			{Topic: "orders", Partition: 0, Earliest: 0, Latest: 20},
			{Topic: "orders", Partition: 1, Earliest: 0, Latest: 20},
		},
	}
	withExecutor(t, executorWith(t, fake))
	out, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--from-file", path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "REPROCESS 5") || !strings.Contains(out, "SKIP 4") ||
		!strings.Contains(out, "--from-file ") || !strings.Contains(out, "reset.csv") || strings.Contains(out, "with offsets.csv") {
		t.Fatalf("output:\n%s", out)
	}

	if _, _, err := runCmd(t, "--brokers", "x:9092", "--group", "g", "--from-file", path, "--to-offset", "orders:0=1"); err == nil {
		t.Fatal("want error combining --from-file and --to-offset")
	}
}
