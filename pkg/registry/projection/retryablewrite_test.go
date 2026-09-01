package projection

import (
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func testGroupResource() schema.GroupResource {
	return schema.GroupResource{Group: "store.example.com", Resource: "orders"}
}

// TestARolledBackWriteIsAConflict is the gap. A serialization failure and a
// deadlock are the two errors a database raises specifically to say no harm was
// done and the write can be sent again — and both came back as 500. A
// controller given 409 requeues and succeeds on the retry; one given 500 treats
// the projection as broken. Anyone using queries.create.statements for a
// check-then-insert meets this under any real concurrency, as does anyone whose
// database runs at SERIALIZABLE, which is every CockroachDB.
func TestARolledBackWriteIsAConflict(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"serialization failure", &pgconn.PgError{Code: "40001", Message: "could not serialize access"}},
		{"deadlock detected", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
		{"MySQL deadlock", &mysqldriver.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("executing statement: %w", tc.err)
			got := translateWriteError(wrapped, testGroupResource(), "order-1001", "update")
			if !errors.IsConflict(got) {
				t.Fatalf("translateWriteError() = %v (%T), want Conflict", got, got)
			}
		})
	}
}

// TestARolledBackWriteSaysItCanBeRetriedUnchanged. The status is the same 409 a
// stale resourceVersion produces, and the two ask for opposite things: there
// the client is holding an object that has moved on and has to read it again,
// here the write was never applied and can be sent exactly as it was. Only the
// cause can say which, so it has to.
func TestARolledBackWriteSaysItCanBeRetriedUnchanged(t *testing.T) {
	got := translateWriteError(
		&pgconn.PgError{Code: "40001", Message: "could not serialize access"},
		testGroupResource(), "order-1001", "update")

	message := got.Error()
	if !strings.Contains(message, "retried unchanged") {
		t.Errorf("the error does not say the write can be sent again as it is: %v", got)
	}
	if strings.Contains(message, "apply your changes to the latest version") {
		t.Errorf("the error reads as a stale-version conflict, which it is not: %v", got)
	}
}

// TestTheOtherWriteFailuresKeepTheirAnswers, so the new case cannot have taken
// anything from the ones that were already right.
func TestTheOtherWriteFailuresKeepTheirAnswers(t *testing.T) {
	gr := testGroupResource()

	unique := translateWriteError(&pgconn.PgError{Code: "23505"}, gr, "order-1001", "create")
	if !errors.IsAlreadyExists(unique) {
		t.Errorf("a unique violation = %v, want AlreadyExists", unique)
	}

	timeout := translateWriteError(&pgconn.PgError{Code: "57014"}, gr, "order-1001", "update")
	if errors.IsConflict(timeout) {
		t.Errorf("a statement timeout = %v, want the timeout answer", timeout)
	}

	down := translateWriteError(fmt.Errorf("dial tcp: connection refused"), gr, "order-1001", "update")
	if !errors.IsServiceUnavailable(down) {
		t.Errorf("an unreachable database = %v, want ServiceUnavailable", down)
	}
}
