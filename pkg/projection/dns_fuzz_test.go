package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

// FuzzIsDNS1123Subdomain is the whole contract for the hand-written scan: it
// exists to be faster than apimachinery's regular expression, not to answer
// differently. A name it accepted and apimachinery did not would be a row
// mapped into an object the API server then refuses.
func FuzzIsDNS1123Subdomain(f *testing.F) {
	for _, seed := range []string{
		"", "a", "order-1001", "a.b.c", "0", "9-9",
		"-a", "a-", ".a", "a.", "a..b", "a-.b", "a.-b",
		"A", "a_b", "a b", "a/b", "café", "\x00",
		"this-is-a-very-long-label-that-is-still-fine",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, name string) {
		want := len(validation.IsDNS1123Subdomain(name)) == 0
		if got := isDNS1123Subdomain(name); got != want {
			t.Errorf("isDNS1123Subdomain(%q) = %v, apimachinery says %v", name, got, want)
		}
	})
}

// TestIsDNS1123SubdomainLength pins the bound the fuzzer is unlikely to hit,
// since a name of exactly 253 characters is not something random input finds.
func TestIsDNS1123SubdomainLength(t *testing.T) {
	long := make([]byte, dns1123SubdomainMaxLength)
	for i := range long {
		long[i] = 'a'
	}

	if !isDNS1123Subdomain(string(long)) {
		t.Errorf("a name of exactly %d characters was rejected", dns1123SubdomainMaxLength)
	}
	if isDNS1123Subdomain(string(append(long, 'a'))) {
		t.Errorf("a name of %d characters was accepted", dns1123SubdomainMaxLength+1)
	}
	// And apimachinery agrees about both.
	for _, name := range []string{string(long), string(append(long, 'a'))} {
		want := len(validation.IsDNS1123Subdomain(name)) == 0
		if got := isDNS1123Subdomain(name); got != want {
			t.Errorf("length %d: got %v, apimachinery says %v", len(name), got, want)
		}
	}
}
