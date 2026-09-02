package sql

import (
	"strings"
	"testing"
)

// A bearer token may only go to a server the connection established the
// identity of.
//
// Encryption alone is not that. PostgreSQL's `require` encrypts and then
// accepts any certificate — pgx sets InsecureSkipVerify for it, exactly as it
// does for MySQL's `skip-verify`, which this package already refuses by name.
// An encrypted connection to an impersonator hands over the token as surely as
// a plaintext one, and more quietly, because everything looks secured.
func TestOnlyVerifiedModesCarryAMintedCredential(t *testing.T) {
	for _, tc := range []struct {
		driver, dsn string
		encrypted   bool
		verified    bool
	}{
		// The mode every example in this repository used to recommend. It
		// encrypts, so it passes the warning; it verifies nothing, so it must
		// not carry a token.
		{"postgres", "postgres://u@h/db?sslmode=require", true, false},
		// Checks the chain, deliberately not the host name — any certificate
		// under the same authority satisfies it, which for a managed database
		// is any instance in the fleet.
		{"postgres", "postgres://u@h/db?sslmode=verify-ca", true, false},
		{"postgres", "postgres://u@h/db?sslmode=verify-full", true, true},
		{"postgres", "postgres://u@h/db?sslmode=disable", false, false},
		{"postgres", "postgres://u@h/db", false, false},
		{"cockroach", "postgres://u@h/db?sslmode=require", true, false},
		{"cockroach", "postgres://u@h/db?sslmode=verify-full", true, true},

		// MySQL was already right: the modes that encrypt without verifying are
		// refused by name, so the two questions have the same answer there.
		{"mysql", "u@tcp(h:3306)/db?tls=true", true, true},
		{"mysql", "u@tcp(h:3306)/db?tls=skip-verify", false, false},
		{"mysql", "u@tcp(h:3306)/db?tls=preferred", false, false},
		{"mysql", "u@tcp(h:3306)/db?tls=custom-ca", true, true},
		{"mysql", "u@tcp(h:3306)/db", false, false},

		// A driver this build cannot reason about does not get the benefit of
		// the doubt.
		{"oracle", "whatever?ssl=on", false, false},
	} {
		t.Run(tc.driver+" "+tc.dsn, func(t *testing.T) {
			d, ok := Lookup(tc.driver)
			if ok && d.Encrypted != nil {
				if got := d.Encrypted(tc.dsn); got != tc.encrypted {
					t.Errorf("Encrypted = %v, want %v", got, tc.encrypted)
				}
			}
			if got := serverVerified(tc.driver, tc.dsn); got != tc.verified {
				t.Errorf("serverVerified = %v, want %v", got, tc.verified)
			}
		})
	}
}

// The two questions must not collapse back into one: every mode that verifies
// also encrypts, and at least one mode encrypts without verifying. If that stops
// being true the second check has become decoration.
func TestVerifiedIsStrictlyStrongerThanEncrypted(t *testing.T) {
	postgres, _ := Lookup("postgres")

	const encryptsOnly = "postgres://u@h/db?sslmode=require"
	if !postgres.Encrypted(encryptsOnly) || serverVerified("postgres", encryptsOnly) {
		t.Error("sslmode=require no longer distinguishes the two checks")
	}

	const verifies = "postgres://u@h/db?sslmode=verify-full"
	if !postgres.Encrypted(verifies) || !serverVerified("postgres", verifies) {
		t.Error("sslmode=verify-full must satisfy both")
	}
}

// The refusal has to say what to do, and name the setting.
func TestVerificationHintNamesTheSetting(t *testing.T) {
	for driver, want := range map[string]string{
		"postgres":  "sslmode=verify-full",
		"cockroach": "sslmode=verify-full",
		"mysql":     "tls=true",
	} {
		if got := verificationHint(driver); !strings.Contains(got, want) {
			t.Errorf("verificationHint(%q) = %q, want it to name %q", driver, got, want)
		}
	}
}
