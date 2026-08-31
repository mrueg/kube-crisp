//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

// The kubectl plugin generates RBAC, reports who may reach a projected kind,
// and finds the roles a deleted projection left behind. Its unit tests check
// that the roles say what the projections mean; only a cluster can check that
// what they say is what the kube-apiserver then does.
//
// Three claims are worth a cluster:
//
//   - a generated view role authorizes reads and refuses writes, and the edit
//     role authorizes the writes — not "grants the verbs", but the request
//     actually succeeding and actually being refused;
//   - `can-i` marking a verb granted-but-not-served is telling the truth, which
//     is checkable by making the request and seeing the 405 it predicts;
//   - `prune` removes an orphaned role and leaves everything else alone.

var (
	pluginOnce sync.Once
	pluginPath string
	pluginErr  error
)

// plugin builds kubectl-crisp once for the whole package and returns its path.
//
// Built rather than `go run` per call: the three tests below invoke it a dozen
// times between them, and `go run` recompiles on each.
func plugin(t *testing.T) string {
	t.Helper()

	pluginOnce.Do(func() {
		dir, err := os.MkdirTemp("", "kubectl-crisp")
		if err != nil {
			pluginErr = err
			return
		}
		pluginPath = filepath.Join(dir, "kubectl-crisp")

		build := exec.Command("go", "build", "-o", pluginPath, "../../cmd/kubectl-crisp")
		if out, err := build.CombinedOutput(); err != nil {
			pluginErr = fmt.Errorf("building kubectl-crisp: %w\n%s", err, out)
		}
	})

	if pluginErr != nil {
		t.Fatal(pluginErr)
	}
	return pluginPath
}

// runPlugin runs the plugin against the e2e cluster, returning stdout.
//
// stderr is returned separately rather than merged: the commands put the data
// on stdout and the prose on stderr precisely so that one can be parsed while
// the other is read, and a test that merged them would not notice if that
// stopped being true.
func runPlugin(t *testing.T, args ...string) (string, string) {
	t.Helper()

	cmd := exec.Command(plugin(t), append(args, "--kubeconfig", kubeconfigPath)...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl-crisp %s: %v\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

// decodeRoles parses the plugin's YAML stream.
func decodeRoles(t *testing.T, out string) map[string]rbacv1.ClusterRole {
	t.Helper()

	roles := map[string]rbacv1.ClusterRole{}
	for _, document := range strings.Split(out, "\n---\n") {
		if strings.TrimSpace(document) == "" {
			continue
		}
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal([]byte(document), &role); err != nil {
			t.Fatalf("parsing generated role: %v\n%s", err, document)
		}
		roles[role.Name] = role
	}
	return roles
}

// applyRoles creates the generated roles and removes them again afterwards.
func applyRoles(t *testing.T, cluster kubernetes.Interface, roles map[string]rbacv1.ClusterRole) {
	t.Helper()
	ctx := context.Background()

	for name := range roles {
		role := roles[name]
		t.Cleanup(func() {
			_ = cluster.RbacV1().ClusterRoles().Delete(context.Background(), role.Name, metav1.DeleteOptions{})
		})
		if _, err := cluster.RbacV1().ClusterRoles().Create(ctx, &role, metav1.CreateOptions{}); err != nil {
			t.Fatalf("applying %s: %v", role.Name, err)
		}
	}
}

// subject creates a ServiceAccount bound to the named ClusterRoles and returns
// a client config authenticating as it.
//
// A real subject with a real token rather than impersonation, because
// impersonation is itself a permission and the question here is what the
// generated role grants on its own.
func subject(t *testing.T, cluster kubernetes.Interface, name string, roleNames ...string) *rest.Config {
	t.Helper()
	ctx := context.Background()

	t.Cleanup(func() {
		_ = cluster.CoreV1().ServiceAccounts(acmeNamespace).Delete(
			context.Background(), name, metav1.DeleteOptions{})
	})
	if _, err := cluster.CoreV1().ServiceAccounts(acmeNamespace).Create(ctx,
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: acmeNamespace}},
		metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the service account: %v", err)
	}

	for _, roleName := range roleNames {
		bindingName := fmt.Sprintf("%s-%s", name, strings.NewReplacer(":", "-", ".", "-").Replace(roleName))
		t.Cleanup(func() {
			_ = cluster.RbacV1().ClusterRoleBindings().Delete(
				context.Background(), bindingName, metav1.DeleteOptions{})
		})

		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: bindingName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: roleName},
			Subjects: []rbacv1.Subject{
				{Kind: "ServiceAccount", Name: name, Namespace: acmeNamespace},
			},
		}
		if _, err := cluster.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("binding %s: %v", roleName, err)
		}
	}

	hour := int64(3600)
	token, err := cluster.CoreV1().ServiceAccounts(acmeNamespace).CreateToken(ctx, name,
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &hour}},
		metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("minting a token: %v", err)
	}

	cfg := rest.CopyConfig(restConfig)
	cfg.BearerToken = token.Status.Token
	cfg.BearerTokenFile = ""
	// The admin credentials would otherwise win over the token.
	cfg.CertFile, cfg.KeyFile = "", ""
	cfg.CertData, cfg.KeyData = nil, nil
	cfg.Username, cfg.Password = "", ""
	return cfg
}

func subjectName(name string) string {
	return fmt.Sprintf("system:serviceaccount:%s:%s", acmeNamespace, name)
}

// TestPluginGeneratedRolesAuthorizeWhatTheyClaim applies the generated roles and
// makes the requests they are about.
//
// The unit tests check that the role names the verbs the projection serves.
// This checks the half they cannot: that the kube-apiserver, handed that role,
// then admits exactly those requests — which is the only reason to generate a
// role rather than describe one.
func TestPluginGeneratedRolesAuthorizeWhatTheyClaim(t *testing.T) {
	ctx := context.Background()
	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("building a cluster client: %v", err)
	}

	stdout, _ := runPlugin(t, "rbac", "orders")
	roles := decodeRoles(t, stdout)

	const (
		viewRole = "kube-crisp:store.example.com:view"
		editRole = "kube-crisp:store.example.com:edit"
	)
	for _, name := range []string{viewRole, editRole} {
		if _, ok := roles[name]; !ok {
			t.Fatalf("no %s in the generated output:\n%s", name, stdout)
		}
	}
	applyRoles(t, cluster, roles)

	// The reader. Bound to the view role and nothing else.
	readerCfg := subject(t, cluster, "crisp-plugin-reader", viewRole)
	reader, err := dynamic.NewForConfig(readerCfg)
	if err != nil {
		t.Fatalf("building the reader client: %v", err)
	}
	readerOrders := reader.Resource(ordersGVR).Namespace(acmeNamespace)

	if _, err := readerOrders.List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("the view role did not authorize a list: %v", err)
	}

	// Cleaned up before it is attempted, not after. This create is meant to be
	// refused, so nothing here removes the row — and the run where it is not
	// refused is exactly the run that leaves one behind, which fails every
	// later run of the suite on its fixture counts. That is the failure this
	// test exists to catch, so it must not also cause a second one.
	const denied = "order-plugin-denied"
	t.Cleanup(func() {
		_ = dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
			Delete(context.Background(), denied, metav1.DeleteOptions{})
	})

	_, err = readerOrders.Create(ctx, newPluginOrder(denied), metav1.CreateOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("the view role authorized a create: %v", err)
	}

	// The writer. Bound to both, since edit carries only the verbs view does
	// not — the pair is what a person who may change things holds.
	writerCfg := subject(t, cluster, "crisp-plugin-writer", viewRole, editRole)
	writer, err := dynamic.NewForConfig(writerCfg)
	if err != nil {
		t.Fatalf("building the writer client: %v", err)
	}
	writerOrders := writer.Resource(ordersGVR).Namespace(acmeNamespace)

	const created = "order-plugin-rbac"
	t.Cleanup(func() {
		// Through the admin client: the writer's bindings are removed by an
		// earlier cleanup, and a fixture left behind fails every later run.
		_ = dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
			Delete(context.Background(), created, metav1.DeleteOptions{})
	})

	if _, err := writerOrders.Create(ctx, newPluginOrder(created), metav1.CreateOptions{}); err != nil {
		t.Fatalf("the edit role did not authorize a create: %v", err)
	}
	if err := writerOrders.Delete(ctx, created, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("the edit role did not authorize a delete: %v", err)
	}
}

// TestPluginCanIReportsWhatTheClusterAndServerAgreeOn checks the finding the
// command exists for, and then checks that the finding is true.
//
// can-i marks a verb granted-but-not-served when RBAC allows it and the
// projection has no query for it, and says the request returns 405. Nothing
// about that is worth trusting on the strength of the two halves agreeing:
// the test makes the request.
func TestPluginCanIReportsWhatTheClusterAndServerAgreeOn(t *testing.T) {
	ctx := context.Background()
	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("building a cluster client: %v", err)
	}

	// A role granting create on jsonorders, which is projected read-only: a
	// list and a get statement and nothing else. Written by hand here, because
	// the generator would never produce it — which is the point. Somebody's
	// hand-written role, or one left from a projection that used to be
	// writable, is where this comes from in a real cluster.
	const roleName = "crisp-plugin-overgranted"
	t.Cleanup(func() {
		_ = cluster.RbacV1().ClusterRoles().Delete(context.Background(), roleName, metav1.DeleteOptions{})
	})
	if _, err := cluster.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: roleName},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{jsonOrdersGVR.Group},
			Resources: []string{jsonOrdersGVR.Resource},
			Verbs:     []string{"get", "list", "create"},
		}},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the over-granting role: %v", err)
	}

	const name = "crisp-plugin-overgranted-sa"
	cfg := subject(t, cluster, name, roleName)

	stdout, _ := runPlugin(t, "can-i", "orders-json",
		"--as", subjectName(name), "--namespace", acmeNamespace, "-o", "json")

	var rows []struct {
		Resource string `json:"resource"`
		Verbs    map[string]struct {
			Served  bool `json:"served"`
			Allowed bool `json:"allowed"`
		} `json:"verbs"`
	}
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatalf("parsing can-i output: %v\n%s", err, stdout)
	}
	if len(rows) != 1 || rows[0].Resource != jsonOrdersGVR.Resource {
		t.Fatalf("expected one row for %s, got %+v", jsonOrdersGVR.Resource, rows)
	}

	verbs := rows[0].Verbs
	if got := verbs["create"]; !got.Allowed || got.Served {
		t.Fatalf("create = %+v, want allowed and not served", got)
	}
	if got := verbs["get"]; !got.Allowed || !got.Served {
		t.Fatalf("get = %+v, want allowed and served", got)
	}
	if got := verbs["delete"]; got.Allowed || got.Served {
		t.Fatalf("delete = %+v, want neither", got)
	}

	// And now the claim itself: authorized, and refused by the server.
	overgranted, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("building the over-granted client: %v", err)
	}
	_, err = overgranted.Resource(jsonOrdersGVR).Namespace(acmeNamespace).Create(ctx,
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": jsonOrdersGVR.Group + "/" + jsonOrdersGVR.Version,
			"kind":       "JSONOrder",
			"metadata":   map[string]any{"name": "order-plugin-405"},
			"spec":       map[string]any{"customer": "ada", "totalCents": int64(10)},
		}}, metav1.CreateOptions{})

	if apierrors.IsForbidden(err) {
		t.Fatalf("the request was refused by RBAC, so it never reached the projection: %v", err)
	}
	if !apierrors.IsMethodNotSupported(err) {
		t.Fatalf("creating an over-granted read-only kind: got %v, want 405 Method Not Allowed", err)
	}
}

// TestPluginPruneRemovesOnlyOrphans.
//
// The three cases that matter are the one it must remove and the two it must
// not: a role for a group that is still projected, and a role it never
// generated. The second is the dangerous one — a hand-written role granting a
// projected group is somebody's work, and this command has no claim on it.
func TestPluginPruneRemovesOnlyOrphans(t *testing.T) {
	ctx := context.Background()
	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("building a cluster client: %v", err)
	}

	const (
		orphan    = "kube-crisp:gone.example.com:view"
		live      = "crisp-plugin-live-group"
		unlabeled = "crisp-plugin-hand-written"
	)
	const groupLabel = "crisp.kubecrisp.io/projected-group"

	roles := []*rbacv1.ClusterRole{
		{ObjectMeta: metav1.ObjectMeta{
			Name:   orphan,
			Labels: map[string]string{groupLabel: "gone.example.com"},
		}},
		{ObjectMeta: metav1.ObjectMeta{
			Name:   live,
			Labels: map[string]string{groupLabel: ordersGVR.Group},
		}},
		// No label: whatever it grants, it is not this command's.
		{ObjectMeta: metav1.ObjectMeta{Name: unlabeled}, Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{ordersGVR.Group},
			Resources: []string{"*"},
			Verbs:     []string{"get"},
		}}},
	}
	for _, role := range roles {
		t.Cleanup(func() {
			_ = cluster.RbacV1().ClusterRoles().Delete(context.Background(), role.Name, metav1.DeleteOptions{})
		})
		if _, err := cluster.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating %s: %v", role.Name, err)
		}
	}

	listed, _ := runPlugin(t, "prune")
	if !strings.Contains(listed, orphan) {
		t.Fatalf("prune did not report the orphaned role:\n%s", listed)
	}
	for _, kept := range []string{live, unlabeled} {
		if strings.Contains(listed, kept) {
			t.Fatalf("prune reported %s, which it must leave alone:\n%s", kept, listed)
		}
	}

	// Reporting is not removing: nothing may have gone yet.
	for _, name := range []string{orphan, live, unlabeled} {
		if _, err := cluster.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("prune without --delete removed %s: %v", name, err)
		}
	}

	runPlugin(t, "prune", "--delete")

	if _, err := cluster.RbacV1().ClusterRoles().Get(ctx, orphan, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("prune --delete left the orphaned role: %v", err)
	}
	for _, kept := range []string{live, unlabeled} {
		if _, err := cluster.RbacV1().ClusterRoles().Get(ctx, kept, metav1.GetOptions{}); err != nil {
			t.Fatalf("prune --delete removed %s: %v", kept, err)
		}
	}
}

func newPluginOrder(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(10)},
		"status":     map[string]any{"phase": "pending"},
	}}
}
