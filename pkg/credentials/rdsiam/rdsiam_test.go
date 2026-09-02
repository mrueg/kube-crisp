package rdsiam

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// fakeCredentials stands in for whatever identity the pod actually has. Fixed,
// so that everything derived from it is the same on every run and on every
// machine, and obviously not real, so that nothing here can be mistaken for a
// leaked key.
func fakeCredentials() aws.CredentialsProvider {
	return credentials.NewStaticCredentialsProvider(
		"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")
}

const (
	testEndpoint = "orders.cluster-cdefghij.eu-central-1.rds.amazonaws.com:5432"
	testRegion   = "eu-central-1"
	testUser     = "orders_app"
)

// The token is a SigV4-presigned URL, and RDS checks every part of it. A signer
// that produced something almost right would be indistinguishable here from one
// that produced something right — until the database rejected it, at which
// point the only evidence is "password authentication failed".
//
// So this asserts the shape, against a fake identity and the real signer.
func TestTheTokenIsTheSignedURLRDSExpects(t *testing.T) {
	signer := &signer{
		endpoint: testEndpoint,
		region:   testRegion,
		user:     testUser,
		creds:    fakeCredentials(),
		build:    auth.BuildAuthToken,
		now:      time.Now,
	}

	token, err := signer.Password(context.Background())
	if err != nil {
		t.Fatalf("Password() returned error: %v", err)
	}

	// No scheme, because it is used as a password and not fetched. The host and
	// port are the endpoint's, so a token signed for one database cannot be
	// presented to another.
	if strings.Contains(token, "://") {
		t.Errorf("the token carries a URL scheme, which RDS does not accept: %s", token)
	}
	if !strings.HasPrefix(token, testEndpoint+"?") {
		t.Fatalf("the token does not address %s: %s", testEndpoint, token)
	}

	query, err := url.ParseQuery(strings.TrimPrefix(token, testEndpoint+"?"))
	if err != nil {
		t.Fatalf("the token is not a URL: %v", err)
	}

	for _, want := range []struct{ key, value string }{
		{"Action", "connect"},
		// The database user, which is what the permission is granted on. RDS
		// signs for one user; this is the one the connection string names.
		{"DBUser", testUser},
		{"X-Amz-Algorithm", "AWS4-HMAC-SHA256"},
		// Fifteen minutes, which is what tokenLifetime below is derived from.
		{"X-Amz-Expires", fmt.Sprint(int(tokenLifetime.Seconds()))},
		{"X-Amz-SignedHeaders", "host"},
	} {
		if got := query.Get(want.key); got != want.value {
			t.Errorf("%s = %q, want %q", want.key, got, want.value)
		}
	}

	// The credential scope names the rds-db service and the database's region.
	// Sign against the wrong one and RDS rejects a token that is otherwise
	// perfectly well formed.
	scope := query.Get("X-Amz-Credential")
	for _, want := range []string{"AKIAIOSFODNN7EXAMPLE/", "/" + testRegion + "/rds-db/aws4_request"} {
		if !strings.Contains(scope, want) {
			t.Errorf("X-Amz-Credential = %q, want it to contain %q", scope, want)
		}
	}
	if query.Get("X-Amz-Signature") == "" {
		t.Error("the token carries no signature")
	}
	if query.Get("X-Amz-Date") == "" {
		t.Error("the token carries no signing date, so RDS cannot tell when it was issued")
	}
}

// A cached token that outlives its validity is the worst failure this code has
// available: an intermittent authentication error, under load, on a connection
// nobody was watching being opened.
//
// So the token is held for less than RDS will accept it — the margin covers a
// slow handshake, a retry, and clock skew — and this test moves a clock across
// both edges to say so.
func TestATokenIsReusedWithinItsMarginAndNeverPastIt(t *testing.T) {
	clock := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	var signed int

	signer := &signer{
		endpoint: testEndpoint,
		region:   testRegion,
		user:     testUser,
		creds:    fakeCredentials(),
		now:      func() time.Time { return clock },
		build: func(context.Context, string, string, string, aws.CredentialsProvider,
			...func(*auth.BuildAuthTokenOptions)) (string, error) {
			signed++
			return fmt.Sprintf("token-%d", signed), nil
		},
	}

	ctx := context.Background()
	first, err := signer.Password(ctx)
	if err != nil {
		t.Fatalf("Password() returned error: %v", err)
	}

	// A burst of connections refilling an idle pool shares one signature rather
	// than signing eight.
	for i := 0; i < 8; i++ {
		if got, _ := signer.Password(ctx); got != first {
			t.Fatalf("a connection opened in the same instant got %q, want the token already in hand", got)
		}
	}
	if signed != 1 {
		t.Errorf("%d tokens were signed for nine connections opened at once, want one", signed)
	}

	// Still inside the margin: the same token, because it is still good.
	clock = clock.Add(tokenLifetime - tokenMargin - time.Second)
	if got, _ := signer.Password(ctx); got != first {
		t.Errorf("a token still comfortably valid was replaced: got %q, want %q", got, first)
	}

	// Past the margin, and well before RDS would refuse it. A new one.
	clock = clock.Add(2 * time.Second)
	second, err := signer.Password(ctx)
	if err != nil {
		t.Fatalf("Password() returned error: %v", err)
	}
	if second == first {
		t.Fatal("the token was reused past the point where a connection could outlive it")
	}
	if signed != 2 {
		t.Errorf("%d tokens were signed in total, want two", signed)
	}

	// And the margin is a margin: the token is dropped strictly before RDS
	// stops accepting it, or there is no margin at all.
	if tokenMargin <= 0 || tokenMargin >= tokenLifetime {
		t.Fatalf("the refresh margin is %s of a %s lifetime, which leaves no room for a slow handshake",
			tokenMargin, tokenLifetime)
	}
}

// A signing failure has to arrive as a signing failure. Swallowed, it becomes an
// authentication error against the database — which points at the database, at
// the user, and at everything except the IAM policy that is actually missing.
func TestASigningFailureSaysWhatItWasSigningFor(t *testing.T) {
	refused := errors.New("AccessDenied: not authorized to perform rds-db:connect")

	signer := &signer{
		endpoint: testEndpoint,
		region:   testRegion,
		user:     testUser,
		creds:    fakeCredentials(),
		now:      time.Now,
		build: func(context.Context, string, string, string, aws.CredentialsProvider,
			...func(*auth.BuildAuthTokenOptions)) (string, error) {
			return "", refused
		},
	}

	_, err := signer.Password(context.Background())
	if err == nil {
		t.Fatal("Password() returned a token from a signer that failed")
	}
	if !errors.Is(err, refused) {
		t.Errorf("Password() returned %v, want the signing error", err)
	}
	for _, want := range []string{testUser, testEndpoint, testRegion} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say it was signing for %q", err, want)
		}
	}

	// And a failure is not cached as though it were a token.
	if signer.token != "" {
		t.Error("a failed signature was cached")
	}
}

// The endpoint and the user come out of the connection string the Secret
// already carries, through the drivers' own parsers, so a projection states them
// once. Getting either wrong signs a valid token for the wrong thing, which RDS
// rejects with nothing pointing at the cause.
func TestTheEndpointAndUserAreReadOutOfTheConnectionString(t *testing.T) {
	for _, tc := range []struct {
		name            string
		driver, dsn     string
		endpoint, user  string
		wantErrContains string
	}{
		{
			name: "a PostgreSQL URL", driver: "postgres",
			dsn:      "postgres://orders_app@orders.eu-central-1.rds.amazonaws.com:5432/store?sslmode=require",
			endpoint: "orders.eu-central-1.rds.amazonaws.com:5432", user: "orders_app",
		},
		{
			name: "the keyword form PostgreSQL also takes", driver: "postgres",
			dsn:      "host=orders.eu-central-1.rds.amazonaws.com port=5432 user=orders_app sslmode=require",
			endpoint: "orders.eu-central-1.rds.amazonaws.com:5432", user: "orders_app",
		},
		{
			name: "CockroachDB, which is the same string", driver: "cockroach",
			dsn:      "postgres://orders_app@crdb.example:26257/store?sslmode=verify-full",
			endpoint: "crdb.example:26257", user: "orders_app",
		},
		{
			name: "MySQL, which is not", driver: "mysql",
			dsn:      "orders_app@tcp(orders.eu-central-1.rds.amazonaws.com:3306)/store?tls=true",
			endpoint: "orders.eu-central-1.rds.amazonaws.com:3306", user: "orders_app",
		},
		{
			name: "a driver this provider has never heard of", driver: "clickhouse",
			dsn:             "clickhouse://orders@db:9000/store",
			wantErrContains: "endpoint",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, user, err := addressOf(crispsql.CredentialRequest{Driver: tc.driver, DSN: tc.dsn})
			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("addressOf() read %s/%s out of a string it cannot parse", endpoint, user)
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error %q does not mention %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("addressOf() returned error: %v", err)
			}
			if endpoint != tc.endpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, tc.endpoint)
			}
			if user != tc.user {
				t.Errorf("user = %q, want %q", user, tc.user)
			}
		})
	}
}

// An RDS endpoint already says which region it is in, so a projection should not
// have to say it again. Anything that is not one yields nothing, and the region
// option and the pod's own AWS configuration still have their turn.
func TestTheRegionIsReadOffAnRDSEndpointWhenItIsOne(t *testing.T) {
	for dsn, want := range map[string]string{
		"orders.cdefghij.eu-central-1.rds.amazonaws.com:5432": "eu-central-1",
		"orders.cdefghij.us-east-1.rds.amazonaws.com":         "us-east-1",
		// Aurora, and the reader endpoint, which are the same shape.
		"orders.cluster-cdefghij.ap-southeast-2.rds.amazonaws.com:5432":    "ap-southeast-2",
		"orders.cluster-ro-cdefghij.ap-southeast-2.rds.amazonaws.com:5432": "ap-southeast-2",
		// China, where the suffix differs and the position does not.
		"orders.cdefghij.cn-north-1.rds.amazonaws.com.cn:5432": "cn-north-1",

		// Not RDS, and not guessed at.
		"db.internal.example:5432": "",
		"localhost:5432":           "",
		"rds.amazonaws.com:5432":   "",
	} {
		if got := regionOf(dsn); got != want {
			t.Errorf("regionOf(%q) = %q, want %q", dsn, got, want)
		}
	}
}

// Options are the one place a provider can need configuration without an API
// change, so the API cannot check them and this has to. A misspelt key that is
// silently ignored is a projection signing tokens for the wrong region, and
// nothing about the failure would lead anybody back to the typo.
func TestAnOptionThisProviderDoesNotUnderstandIsRefused(t *testing.T) {
	if err := checkOptions(map[string]string{"region": "eu-central-1", "user": "orders", "endpoint": "db:5432"}); err != nil {
		t.Errorf("checkOptions() rejected the options this provider documents: %v", err)
	}

	err := checkOptions(map[string]string{"region": "eu-central-1", "Region": "us-east-1", "profile": "prod"})
	if err == nil {
		t.Fatal("checkOptions() accepted options this provider ignores")
	}
	for _, want := range []string{"Region", "profile", "region", "user", "endpoint"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// The provider has to actually plug into the seam it was written for: registered
// under the name a projection carries, and reachable through an ordinary pool.
//
// Nothing here connects to anything — Open builds the pool and does not dial —
// but it does go through the whole path a projection takes, including the
// refusal of a connection string that does not ask for TLS.
func TestTheProviderRegistersAndOpensAPool(t *testing.T) {
	// The default credential chain reads the environment. Pinned, so that this
	// test does the same thing on a laptop with an AWS profile as it does in CI
	// with none — and so that nothing here reaches for instance metadata on a
	// machine that has none to give.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", testRegion)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

	if err := Register(); err != nil {
		t.Fatalf("Register() returned error: %v", err)
	}
	if _, ok := crispsql.LookupCredentialProvider(ProviderName); !ok {
		t.Fatalf("the provider is not registered under %q", ProviderName)
	}
	if err := Register(); err == nil {
		t.Error("registering the provider twice was accepted")
	}

	const dsn = "postgres://orders_app@" + testEndpoint + "/store?sslmode=verify-full"

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver: "postgres",
		DSN:    dsn,
		Auth:   &crispsql.AuthOptions{Provider: ProviderName},
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	_ = pool.Close()

	// And the same data source without TLS is refused, because a token is a
	// bearer credential and RDS would refuse the connection anyway.
	plaintext := strings.Replace(dsn, "sslmode=verify-full", "sslmode=disable", 1)
	if _, err := crispsql.Open(crispsql.PoolOptions{
		Driver: "postgres",
		DSN:    plaintext,
		Auth:   &crispsql.AuthOptions{Provider: ProviderName},
	}); err == nil {
		t.Error("a pool minting IAM tokens was opened on an unencrypted connection")
	}
}
