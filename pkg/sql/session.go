package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SessionVariable is one setting applied to the connection a query runs on,
// before it runs.
//
// This is what lets a database enforce the tenancy boundary itself. With
// PostgreSQL row-level security, a policy reading current_setting('app.tenant')
// decides which rows exist for the query — so a mistake in a projection's WHERE
// clause cannot hand one tenant another's rows, because the database never
// offered them.
type SessionVariable struct {
	Name  string
	Value string
}

// ValidateSessionVariableName rejects anything that would not be safe to place
// in a statement.
//
// The name cannot be a bind parameter in any of the supported drivers, so it
// goes into the statement text and has to be beyond suspicion: letters, digits,
// underscore, and the dot PostgreSQL uses to namespace custom settings.
//
// Scanned by hand rather than with a regular expression because it runs on
// every query of every projection that sets a variable, and the answer for a
// given name never changes.
func ValidateSessionVariableName(name string) error {
	invalid := fmt.Errorf(
		"session variable name %q must be letters, digits and underscores, optionally dotted", name)

	if name == "" {
		return invalid
	}
	atSegmentStart := true
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '.':
			// A dot separates segments and may neither start nor end one.
			if atSegmentStart {
				return invalid
			}
			atSegmentStart = true
		case isNameStart(c):
			atSegmentStart = false
		case c >= '0' && c <= '9':
			// A segment may not begin with a digit, matching an identifier.
			if atSegmentStart {
				return invalid
			}
		default:
			return invalid
		}
	}
	if atSegmentStart {
		return invalid
	}
	return nil
}

// applyStatementTimeout bounds the work inside this transaction at the database.
//
// Cancelling a context stops this server waiting for an answer; it does not
// reliably stop PostgreSQL producing one. A statement that picks the wrong plan
// keeps burning the database's CPU long after the client that asked for it gave
// up, and every request behind it starts further behind. This makes the deadline
// the query already has mean something at both ends.
//
// SET LOCAL, so it dies with the transaction rather than outliving it on a
// connection that goes back to a pool every projection reaching this database
// shares. The value is bound rather than written into the statement.
func (p *Pool) applyStatementTimeout(ctx context.Context, tx *sql.Tx, enforced bool, timeout time.Duration) error {
	if !enforced || timeout <= 0 {
		return nil
	}

	// Rounded up: a sub-millisecond budget is zero to PostgreSQL, and zero
	// means no limit — the opposite of what was asked for.
	milliseconds := (timeout + time.Millisecond - 1) / time.Millisecond

	if _, err := tx.ExecContext(ctx,
		p.rewritten("SELECT set_config('statement_timeout', :timeout, true)"),
		strconv.FormatInt(int64(milliseconds), 10),
	); err != nil {
		return fmt.Errorf("setting the statement timeout: %w", err)
	}
	return nil
}

// Bounds on how much longer this server waits than the database does.
const (
	minStatementTimeoutGrace = 100 * time.Millisecond
	maxStatementTimeoutGrace = time.Second
)

// clientDeadline is how long to wait for a query whose deadline the database is
// enforcing too.
//
// Deliberately a little longer than the database's. Set the two to the same
// value and they race — and this server's own deadline usually wins, so the
// caller is told "context deadline exceeded" and whether the database actually
// stopped working depends on a cancellation request landing behind it. With the
// grace the database aborts first and says so, which is both the more useful
// answer and the one that proves the bound is enforced where it matters.
//
// Without a server-side timeout there is nothing to wait for and the query's own
// deadline stands unchanged.
func clientDeadline(enforced bool, timeout time.Duration) time.Duration {
	if !enforced || timeout <= 0 {
		return timeout
	}
	grace := min(max(timeout/10, minStatementTimeoutGrace), maxStatementTimeoutGrace)
	return timeout + grace
}

// applySession sets the variables on one connection, inside the transaction
// that will run the query.
//
// The values are always bound, never interpolated. PostgreSQL takes the name as
// a parameter too, through set_config; MySQL does not, which is why the name is
// validated before it is ever used.
func (p *Pool) applySession(ctx context.Context, tx *sql.Tx, session []SessionVariable) error {
	for _, variable := range session {
		if err := ValidateSessionVariableName(variable.Name); err != nil {
			return err
		}

		switch p.driver {
		case "postgres":
			// The third argument makes it local to the transaction, so the
			// setting cannot outlive the query on a pooled connection.
			if _, err := tx.ExecContext(ctx,
				p.rewritten("SELECT set_config(:name, :value, true)"),
				variable.Name, variable.Value,
			); err != nil {
				return fmt.Errorf("setting session variable %q: %w", variable.Name, err)
			}
		case "mysql":
			// MySQL has no transaction-local settings, so this is a user
			// variable on the connection — and it outlives the transaction.
			// clearSession puts it back to NULL before the transaction ends,
			// because the connection goes back to a pool that every projection
			// reaching this database shares, and the next request on it may
			// belong to a projection that sets nothing.
			if _, err := tx.ExecContext(ctx,
				fmt.Sprintf("SET @%s = ?", mysqlVariableName(variable.Name)),
				variable.Value,
			); err != nil {
				return fmt.Errorf("setting session variable %q: %w", variable.Name, err)
			}
		default:
			return fmt.Errorf("driver %q has no session variables", p.driver)
		}
	}
	return nil
}

// clearSession puts connection-scoped variables back before the transaction
// ends.
//
// Only MySQL needs it. PostgreSQL's set_config is called with the local flag,
// so its settings are already scoped to the transaction and disappear with it.
// MySQL user variables belong to the connection, and the connection is returned
// to a pool shared by every projection reaching the same database — so a value
// left behind is one a later query, from a different projection and a different
// request, could still read.
//
// A failure here is reported, because a variable that could not be cleared is
// exactly the condition this exists to prevent.
func (p *Pool) clearSession(ctx context.Context, tx *sql.Tx, session []SessionVariable) error {
	if p.driver != "mysql" {
		return nil
	}

	for _, variable := range session {
		if err := ValidateSessionVariableName(variable.Name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("SET @%s = NULL", mysqlVariableName(variable.Name)),
		); err != nil {
			return fmt.Errorf("clearing session variable %q: %w", variable.Name, err)
		}
	}
	return nil
}

// mysqlVariableName is what a dotted setting is called as a MySQL user
// variable, which cannot contain a dot.
func mysqlVariableName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

// rewritten turns a :named statement into this driver's placeholder syntax. It
// is only used for the fixed statements above, whose parameters are known.
func (p *Pool) rewritten(stmt string) string {
	out, _, err := Rewrite(stmt, p.driver)
	if err != nil {
		// The statements this is called with are constants, so a failure here
		// is a programming error rather than a configuration one.
		return stmt
	}
	return out
}
