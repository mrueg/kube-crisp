// Package rdsiam authenticates a kube-crisp data source against Amazon RDS with
// an IAM token instead of a stored password.
//
// RDS will accept, in place of a password, a URL signed with SigV4 by an AWS
// identity that holds rds-db:connect on the database user. It is valid for
// fifteen minutes. That is the whole scheme: there is no extra service to run
// and nothing to install next to the database — which is why it is the first
// credential provider kube-crisp ships, and the shape every other cloud's is
// expected to follow.
//
// What it needs from the operator is an identity with that permission (on EKS,
// a service account annotated for IRSA is the usual way) and a database user
// created with the rds_iam role. What it needs from the projection is the
// connection string it would have had anyway, minus the password:
//
//	dataSource:
//	  driver: postgres
//	  secretRef: {name: orders-db, namespace: kube-crisp}
//	  auth:
//	    provider: aws-rds-iam
//	    options:
//	      region: eu-central-1     # optional; read off the endpoint otherwise
//
// Linked into the server rather than kept in a module of its own. It costs
// fifteen AWS SDK modules and about 4 MB of binary that a build projecting a
// SQLite file never reaches, which is a real cost and was weighed: the
// alternative made the published server unable to authenticate to RDS at all,
// so anybody wanting it had to assemble a build first. A provider nobody can
// use without rebuilding is not a provider that ships.
//
// The registry stays open either way. A build wanting a provider this
// repository does not have registers its own the same way cmd does below, and
// pays for that one alone.
package rdsiam

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// ProviderName is what a projection's dataSource.auth.provider carries.
const ProviderName = "aws-rds-iam"

// Register makes this provider available to projections. Call it from main
// before the server starts; see cmd/kube-crisp-apiserver in this module.
func Register() error {
	return crispsql.RegisterCredentialProvider(crispsql.CredentialProvider{
		Name: ProviderName,
		Open: open,
	})
}

// Token lifetime, and how much of it is left unused.
//
// RDS signs for fifteen minutes and refuses a token past that, to the second. A
// cache that held one for the full fifteen would therefore hand out a token that
// expires while the connection carrying it is still shaking hands — and the
// failure would be an intermittent authentication error under load, which is
// about the least diagnosable thing this could do. Five minutes of margin covers
// a slow handshake, a retried connection, and the clock skew between this
// process and the signing endpoint several times over, and costs one extra
// signature every ten minutes on a pool that is opening connections at all.
//
// Signing is local — SigV4 over a URL, no request to AWS — so the cache is not
// about the cost of the token. It is about not signing eight of them when a pool
// refills after an idle period, which is exactly when database/sql opens
// connections in a burst.
const (
	tokenLifetime = 15 * time.Minute
	tokenMargin   = 5 * time.Minute
)

// The options this provider understands. Anything else is a mistake, and is
// reported as one: a misspelt key that is silently ignored is a projection
// running against the wrong region or the wrong user, which is not a failure
// anybody would connect back to a typo.
const (
	optionRegion   = "region"
	optionUser     = "user"
	optionEndpoint = "endpoint"
)

// open builds the credentials for one data source. It runs once, when the pool
// is opened, which is while the projection is being compiled — so everything
// that can be settled here is settled here, and what comes back does nothing but
// sign.
func open(req crispsql.CredentialRequest) (crispsql.Credentials, error) {
	if err := checkOptions(req.Options); err != nil {
		return nil, err
	}

	endpoint, user, err := addressOf(req)
	if err != nil {
		return nil, err
	}
	if override := req.Options[optionEndpoint]; override != "" {
		endpoint = override
	}
	if override := req.Options[optionUser]; override != "" {
		user = override
	}
	if user == "" {
		return nil, fmt.Errorf(
			"the connection string names no database user and no %q option was given; "+
				"RDS signs a token for one particular user", optionUser)
	}

	// Loaded here rather than on the first connection, so that a pod with no
	// usable AWS configuration fails the projection with the SDK's own message
	// while somebody is looking at it — not later, as an authentication failure
	// against the database, which is where nobody would look for it.
	//
	// Bounded, because the default chain can reach for the instance metadata
	// service, and a compile that hangs is worse than one that fails.
	ctx, cancel := context.WithTimeout(context.Background(), configTimeout)
	defer cancel()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving AWS configuration: %w", err)
	}

	region := req.Options[optionRegion]
	if region == "" {
		region = regionOf(endpoint)
	}
	if region == "" {
		region = awsConfig.Region
	}
	if region == "" {
		return nil, fmt.Errorf(
			"no AWS region: %q is not an RDS endpoint a region can be read from, no %q option was given, "+
				"and the AWS configuration in this pod resolves none either",
			endpoint, optionRegion)
	}

	return &signer{
		endpoint: endpoint,
		region:   region,
		user:     user,
		creds:    awsConfig.Credentials,
		build:    auth.BuildAuthToken,
		now:      time.Now,
	}, nil
}

// configTimeout bounds resolving the AWS configuration.
const configTimeout = 10 * time.Second

// checkOptions refuses a key this provider does not understand.
func checkOptions(options map[string]string) error {
	var unknown []string
	for key := range options {
		switch key {
		case optionRegion, optionUser, optionEndpoint:
		default:
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown auth option(s) %s; %s understands %s, %s and %s",
		strings.Join(unknown, ", "), ProviderName, optionEndpoint, optionRegion, optionUser)
}

// addressOf reads the endpoint and the database user out of the connection
// string, so that the projection states them once rather than twice.
//
// Through the drivers' own parsers, because a connection string is not one
// format: PostgreSQL takes a URL or a list of keywords, MySQL takes neither, and
// a hand-rolled reader that got any of it slightly wrong would sign a token for
// the wrong host and produce an authentication failure with nothing pointing at
// the cause.
func addressOf(req crispsql.CredentialRequest) (endpoint, user string, err error) {
	switch req.Driver {
	case "postgres", "cockroach":
		config, err := pgx.ParseConfig(req.DSN)
		if err != nil {
			return "", "", fmt.Errorf("reading the connection string: %w", err)
		}
		return net.JoinHostPort(config.Host, fmt.Sprint(config.Port)), config.User, nil

	case "mysql":
		config, err := mysql.ParseDSN(req.DSN)
		if err != nil {
			return "", "", fmt.Errorf("reading the connection string: %w", err)
		}
		return config.Addr, config.User, nil
	}

	// Reachable only if a build registers a driver this provider has never
	// heard of and points a projection at RDS with it. Better a refusal naming
	// the option than a token signed for an endpoint guessed out of a string
	// nothing here can read.
	return "", "", fmt.Errorf(
		"%s cannot read a %s connection string; give it the %q and %q options instead",
		ProviderName, req.Driver, optionEndpoint, optionUser)
}

// regionOf reads the region out of an RDS endpoint.
//
// They are named <instance>.<account-suffix>.<region>.rds.amazonaws.com, and in
// China and GovCloud the suffix differs but the position does not. It saves
// stating in the projection something the endpoint already says — and an
// endpoint that does not look like RDS at all simply yields nothing, so the
// region option and the pod's own AWS configuration still have their turn.
func regionOf(endpoint string) string {
	host := endpoint
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		host = h
	}

	labels := strings.Split(host, ".")
	for i, label := range labels {
		// rds.amazonaws.com, rds.amazonaws.com.cn, rds.<partition>.amazonaws.com
		if label != "rds" || i == 0 {
			continue
		}
		return labels[i-1]
	}
	return ""
}

// signer mints the token, and holds on to one for as long as it is safely valid.
type signer struct {
	endpoint string
	region   string
	user     string
	creds    aws.CredentialsProvider

	// build is auth.BuildAuthToken, and now is time.Now. Both are fields so
	// that the cache can be tested against a clock the test moves, without
	// waiting fifteen minutes or reaching for AWS.
	build func(ctx context.Context, endpoint, region, user string, creds aws.CredentialsProvider,
		optFns ...func(*auth.BuildAuthTokenOptions)) (string, error)
	now func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

// Password returns a token that is valid now, signing a new one if the one in
// hand is close enough to expiry that a connection might outlive it.
func (s *signer) Password(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Held under the lock while signing, which serialises a burst of
	// connections behind one signature rather than letting each of them sign
	// its own. Signing is local and takes microseconds; the alternative is
	// every connection in a refilling pool doing the same work at the same
	// moment and then throwing all but one of the results away.
	now := s.now()
	if s.token != "" && now.Before(s.expires) {
		return s.token, nil
	}

	token, err := s.build(ctx, s.endpoint, s.region, s.user, s.creds)
	if err != nil {
		return "", fmt.Errorf("signing an RDS IAM token for %s@%s in %s: %w",
			s.user, s.endpoint, s.region, err)
	}

	s.token = token
	s.expires = now.Add(tokenLifetime - tokenMargin)
	return token, nil
}
