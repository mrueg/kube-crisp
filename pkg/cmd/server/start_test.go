package server

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/mrueg/kube-crisp/pkg/apiserver"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

func TestDefaultsSuitAnAggregatedServer(t *testing.T) {
	o := NewCrispServerOptions(os.Stdout, os.Stderr)

	if o.RecommendedOptions.Etcd != nil {
		t.Error("etcd options are set; a projection server stores nothing")
	}
	// Admission options exist so their flags are registered, but the chain
	// only runs when it is asked for: the plugins watch webhook
	// configurations, admission policies, and namespaces cluster-wide.
	if o.EnableAdmission {
		t.Error("admission runs by default; it needs RBAC beyond the base install")
	}
	if o.RecommendedOptions.Features.EnablePriorityAndFairness {
		t.Error("priority and fairness runs by default; it watches flowcontrol objects the base RBAC does not grant")
	}
	if !o.RecommendedOptions.Authentication.RemoteKubeConfigFileOptional ||
		!o.RecommendedOptions.Authorization.RemoteKubeConfigFileOptional {
		t.Error("a kubeconfig is required, so the server cannot run outside a cluster")
	}
	if !o.WatchProjections || !o.APIServices.Enabled {
		t.Error("the dynamic paths are off by default")
	}

	// The aggregation layer fetches these as its own identity, which has no
	// RBAC on this server.
	allowed := strings.Join(o.RecommendedOptions.Authorization.AlwaysAllowPaths, ",")
	for _, path := range []string{"/openapi/v2", "/openapi/v3"} {
		if !strings.Contains(allowed, path) {
			t.Errorf("%s is not in AlwaysAllowPaths (%s)", path, allowed)
		}
	}
}

// TestAdmissionIsOffUnlessAskedFor checks the default all the way through to
// the built configuration, not just the flag.
func TestAdmissionIsOffUnlessAskedFor(t *testing.T) {
	config, err := offlineOptions(t).Config()
	if err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}
	if config.GenericConfig.AdmissionControl != nil {
		t.Error("an admission chain was built without --enable-admission")
	}

	// The flag still has to exist, or enabling it would need a rebuild.
	cmd := NewCommandStartCrispServer(context.Background(), NewCrispServerOptions(os.Stdout, os.Stderr))
	if cmd.Flags().Lookup("enable-admission") == nil {
		t.Error("--enable-admission is not registered")
	}
	for _, name := range []string{"enable-admission-plugins", "disable-admission-plugins"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("--%s is not registered, so the chain cannot be tuned", name)
		}
	}
}

// TestPriorityAndFairnessIsOffUnlessAskedFor checks the flag reaches the
// built configuration, since a filter that silently stays off would look
// exactly like one that is working.
func TestPriorityAndFairnessIsOffUnlessAskedFor(t *testing.T) {
	options := offlineOptions(t)
	if _, err := options.Config(); err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}
	if options.RecommendedOptions.Features.EnablePriorityAndFairness {
		t.Error("the filter was enabled without --enable-priority-and-fairness")
	}

	// The flag is the generic one, and setting it has to survive Config().
	cmd := NewCommandStartCrispServer(context.Background(), NewCrispServerOptions(os.Stdout, os.Stderr))
	flag := cmd.Flags().Lookup("enable-priority-and-fairness")
	if flag == nil {
		t.Fatal("--enable-priority-and-fairness is not registered")
	}
	if flag.DefValue != "false" {
		t.Errorf("--enable-priority-and-fairness defaults to %q, want it off", flag.DefValue)
	}

	// Turning it on has to reach the generic machinery rather than being
	// quietly cleared. These options have no core client — that is what makes
	// them offline — and the filter needs one for its informers, so the
	// complaint about the missing client is the proof that the setting
	// survived. Config() succeeding here would mean the flag does nothing.
	asked := offlineOptions(t)
	asked.RecommendedOptions.Features.EnablePriorityAndFairness = true

	_, err := asked.Config()
	if err == nil {
		t.Fatal("Config() ignored the enabled filter")
	}
	if !strings.Contains(err.Error(), "priority and fairness") {
		t.Errorf("Config() error = %v, want it to name the filter", err)
	}
}

func TestValidateRequiresSomethingToServe(t *testing.T) {
	o := NewCrispServerOptions(os.Stdout, os.Stderr)
	o.WatchProjections = false
	o.ProjectionDir = ""

	err := o.Validate()
	if err == nil {
		t.Fatal("a server with no projection source and no watch was accepted")
	}
	if !strings.Contains(err.Error(), "projection-dir") || !strings.Contains(err.Error(), "watch-projections") {
		t.Errorf("error %q does not say which flag to set", err)
	}
}

func TestValidateAcceptsEitherSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*CrispServerOptions)
	}{
		{"watching", func(o *CrispServerOptions) { o.WatchProjections = true }},
		{"directory", func(o *CrispServerOptions) {
			o.WatchProjections = false
			o.ProjectionDir = t.TempDir()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := NewCrispServerOptions(os.Stdout, os.Stderr)
			tc.apply(o)

			if err := o.Validate(); err != nil {
				t.Fatalf("Validate() returned error: %v", err)
			}
		})
	}
}

func TestDSNResolverSelection(t *testing.T) {
	o := NewCrispServerOptions(os.Stdout, os.Stderr)
	o.LocalDSNFromEnv = true

	resolver, err := o.dsnResolver(genericapiserver.NewRecommendedConfig(apiserver.Codecs))
	if err != nil {
		t.Fatalf("dsnResolver() returned error: %v", err)
	}
	if _, ok := resolver.(projection.EnvDSNResolver); !ok {
		t.Errorf("resolver is %T, want the environment resolver", resolver)
	}
}

// TestDSNResolverNeedsAClusterOrTheEnvironment covers the failure a developer
// hits first: running outside a cluster without saying where credentials come
// from.
func TestDSNResolverNeedsAClusterOrTheEnvironment(t *testing.T) {
	o := NewCrispServerOptions(os.Stdout, os.Stderr)

	// A config with no ClientConfig is what running outside a cluster produces.
	_, err := o.dsnResolver(genericapiserver.NewRecommendedConfig(apiserver.Codecs))
	if err == nil {
		t.Fatal("a resolver was built with neither a kubeconfig nor --local-dsn-from-env")
	}
	if !strings.Contains(err.Error(), "local-dsn-from-env") {
		t.Errorf("error %q does not name the flag that fixes it", err)
	}
}

// TestCABundleIsReadFromDisk checks the flag that replaces
// insecureSkipTLSVerify on generated APIServices.
// offlineOptions returns options that build a config without a cluster and
// without binding a port: secure serving is disabled, so ApplyTo neither
// generates certificates nor listens, and the core API client is not built.
func offlineOptions(t *testing.T) *CrispServerOptions {
	t.Helper()

	o := NewCrispServerOptions(os.Stdout, os.Stderr)
	o.RecommendedOptions.CoreAPI = nil
	o.RecommendedOptions.SecureServing.BindPort = 0
	// Config() generates self-signed certificates, and the default directory is
	// relative to the working directory — which is the package source tree
	// under `go test`. Without this the run leaves a private key in the repo.
	o.RecommendedOptions.SecureServing.ServerCert.CertDirectory = t.TempDir()
	o.LocalDSNFromEnv = true
	o.WatchProjections = false
	o.ProjectionDir = writeProjection(t)
	return o
}

// writeProjection returns a directory holding one valid projection, since a
// server that watches nothing and has nothing on disk has nothing to serve.
func writeProjection(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	manifest := `apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: orders
spec:
  dataSource:
    driver: sqlite
    secretRef: {name: orders-db, namespace: kube-crisp}
  resource:
    group: store.example.com
    version: v1alpha1
    kind: Order
    plural: orders
    scope: Namespaced
    schema:
      type: object
  queries:
    list:
      sql: SELECT id, tenant FROM orders WHERE tenant = :namespace
  mapping:
    name: id
    namespace: tenant
`
	if err := os.WriteFile(filepath.Join(dir, "orders.yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("writing the projection: %v", err)
	}
	return dir
}

func TestCABundleIsReadFromDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatalf("writing the CA file: %v", err)
	}

	o := offlineOptions(t)
	o.APIServiceCABundleFile = path

	config, err := o.Config()
	if err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}
	if len(config.ExtraConfig.APIServices.CABundle) == 0 {
		t.Fatal("the CA bundle was not read")
	}

	// The OpenAPI endpoints have to be configured, or projected schemas have
	// nowhere to be published and kubectl explain returns nothing.
	if config.GenericConfig.OpenAPIV3Config == nil {
		t.Error("OpenAPI v3 is not configured")
	}
	if config.GenericConfig.OpenAPIConfig == nil {
		t.Error("OpenAPI v2 is not configured; the aggregation layer downloads it")
	}
}

// TestConfigRejectsAnEmptyProjectionDirectory: a server that watches nothing
// and finds nothing on disk would start and serve no API at all, which is
// never what was meant.
func TestConfigRejectsAnEmptyProjectionDirectory(t *testing.T) {
	o := offlineOptions(t)
	o.ProjectionDir = t.TempDir()

	_, err := o.Config()
	if err == nil {
		t.Fatal("a server with nothing to serve was accepted")
	}
	if !strings.Contains(err.Error(), "no CustomResourceProjection manifests") {
		t.Errorf("error %q does not explain that the directory is empty", err)
	}
}

// TestConfigLoadsProjectionsFromDisk is the other half: the directory is read
// and the projections reach the server's configuration.
func TestConfigLoadsProjectionsFromDisk(t *testing.T) {
	config, err := offlineOptions(t).Config()
	if err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}

	if got, want := len(config.ExtraConfig.StaticProjections), 1; got != want {
		t.Fatalf("loaded %d projections, want %d", got, want)
	}
	if got, want := config.ExtraConfig.StaticProjections[0].Name, "orders"; got != want {
		t.Errorf("projection = %q, want %q", got, want)
	}
	if config.ExtraConfig.Pools == nil || config.ExtraConfig.DSNResolver == nil {
		t.Error("the configuration carries no pool cache or DSN resolver")
	}
}

// TestServingLimitsReachTheConfig covers the options the recommended set leaves
// out.
//
// They were unreachable until ServerRunOptions was wired in: the flags simply
// did not exist, so an operator could not lower the in-flight bound to match a
// connection pool, and could not ask the server to keep serving for a moment
// after SIGTERM — which for an aggregated server is the difference between a
// rolling update that is invisible and one that 503s every projected group
// while the aggregation layer catches up.
func TestServingLimitsReachTheConfig(t *testing.T) {
	o := offlineOptions(t)

	// Parsed through the command rather than set on the struct, so this covers
	// the flags being registered as well as the values being carried. The
	// command shallow-copies the options it is given, but ServerRun is a
	// pointer the copy shares, so what the flags set is what o holds.
	cmd := NewCommandStartCrispServer(context.Background(), o)
	if err := cmd.Flags().Parse([]string{
		"--shutdown-delay-duration=15s",
		"--shutdown-send-retry-after=true",
		"--max-requests-inflight=40",
		"--max-mutating-requests-inflight=10",
		"--request-timeout=90s",
		"--min-request-timeout=300",
		"--goaway-chance=0.001",
	}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}

	if err := o.Complete(); err != nil {
		t.Fatalf("Complete() returned error: %v", err)
	}
	if err := o.Validate(); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}

	config, err := o.Config()
	if err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}

	generic := config.GenericConfig.Config
	for _, tc := range []struct {
		name      string
		got, want any
	}{
		{"ShutdownDelayDuration", generic.ShutdownDelayDuration, 15 * time.Second},
		{"ShutdownSendRetryAfter", generic.ShutdownSendRetryAfter, true},
		{"MaxRequestsInFlight", generic.MaxRequestsInFlight, 40},
		{"MaxMutatingRequestsInFlight", generic.MaxMutatingRequestsInFlight, 10},
		{"RequestTimeout", generic.RequestTimeout, 90 * time.Second},
		{"MinRequestTimeout", generic.MinRequestTimeout, 300},
		{"GoawayChance", generic.GoawayChance, 0.001},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	// MinRequestTimeout is the one that was already being read: the router is
	// handed it when it installs a projected group, so a value that never left
	// the default meant the knob was wired to nothing.
	if generic.MinRequestTimeout != 300 {
		t.Errorf("the router would be given a MinRequestTimeout of %d", generic.MinRequestTimeout)
	}

	// EffectiveVersion still has to be set, or completing the config panics.
	if config.GenericConfig.EffectiveVersion == nil {
		t.Error("EffectiveVersion is nil, which panics an aggregated server on startup")
	}
}

// TestServingLimitDefaultsAreTheUpstreamOnes: wiring the options in must not
// quietly change how the server behaves when nobody passes a flag.
func TestServingLimitDefaultsAreTheUpstreamOnes(t *testing.T) {
	o := offlineOptions(t)
	if err := o.Complete(); err != nil {
		t.Fatalf("Complete() returned error: %v", err)
	}

	config, err := o.Config()
	if err != nil {
		t.Fatalf("Config() returned error: %v", err)
	}

	defaults := genericapiserver.NewConfig(apiserver.Codecs)
	generic := config.GenericConfig.Config
	if generic.MaxRequestsInFlight != defaults.MaxRequestsInFlight {
		t.Errorf("MaxRequestsInFlight = %d, want the upstream default %d",
			generic.MaxRequestsInFlight, defaults.MaxRequestsInFlight)
	}
	if generic.RequestTimeout != defaults.RequestTimeout {
		t.Errorf("RequestTimeout = %v, want the upstream default %v",
			generic.RequestTimeout, defaults.RequestTimeout)
	}
	if generic.ShutdownDelayDuration != defaults.ShutdownDelayDuration {
		t.Errorf("ShutdownDelayDuration = %v, want the upstream default %v",
			generic.ShutdownDelayDuration, defaults.ShutdownDelayDuration)
	}
}

// TestServingDNSNamesCoverTheService covers the subject alternative names a
// generated certificate has to carry.
//
// The aggregation layer never needed them: an APIService can be created with
// insecureSkipTLSVerify, so nothing verified the name and a certificate naming
// only localhost worked. An admission webhook has no such option, and the
// failure it produces is a webhook that fails open — a check that is not
// running, reported nowhere the person applying a projection would look.
func TestServingDNSNamesCoverTheService(t *testing.T) {
	o := &CrispServerOptions{}
	o.APIServices.ServiceName = "kube-crisp-apiserver"
	o.APIServices.ServiceNamespace = "kube-crisp"

	names := o.servingDNSNames()

	// Every form, because which one is used depends on the caller and on
	// whether the cluster's resolver appends the domain.
	for _, want := range []string{
		"kube-crisp-apiserver",
		"kube-crisp-apiserver.kube-crisp",
		"kube-crisp-apiserver.kube-crisp.svc",
		"kube-crisp-apiserver.kube-crisp.svc.cluster.local",
	} {
		if !slices.Contains(names, want) {
			t.Errorf("%q is not a subject alternative name; a client reaching this server at "+
				"that name gets a certificate error, and a webhook then fails open", want)
		}
	}
}

// TestServingDNSNamesAreEmptyWithoutAService, since running outside a cluster
// there is no Service to name and a certificate should not claim one.
func TestServingDNSNamesAreEmptyWithoutAService(t *testing.T) {
	if names := (&CrispServerOptions{}).servingDNSNames(); len(names) != 0 {
		t.Errorf("servingDNSNames() = %v with no Service configured, want none", names)
	}
}
