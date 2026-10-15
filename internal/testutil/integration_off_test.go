//go:build !integration

package testutil

import "testing"

func TestIntegrationSkippedWithoutTag(t *testing.T) {
	t.Log("integration tests are skipped; run with `-tags=integration` to enable them (requires Docker).")
}
