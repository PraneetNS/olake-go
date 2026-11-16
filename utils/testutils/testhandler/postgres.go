package testhandler

import (
	"context"
	"time"
)

const (
	// serverUser and serverKey log in to every test server that takes credentials.
	serverUser = "olake"
	serverKey  = "olake-testhandler"
)

// SQLDatabase is one test's own database on a shared server, addressed as a client on the host sees it.
type SQLDatabase struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

var postgresService = &service{
	name:  "postgres",
	image: "postgres:15",
	port:  5432,
	env:   []string{"POSTGRES_PASSWORD=" + serverKey},
	args: []string{
		"-c", "wal_level=logical",
		"-c", "max_replication_slots=100",
		"-c", "max_wal_senders=100",
		"-c", "max_connections=300",
	},
	startup: 2 * time.Minute,
	// The probe uses TCP: the image's init-time server listens on the socket only and then restarts.
	ready: func(ctx context.Context, c *container) error {
		_, err := psql(ctx, c, "postgres", "SELECT 1")
		return err
	},
}

// Postgres returns this test's own database on the shared Postgres server, set up for logical replication.
// Cleanup drops the database with every table, publication and replication slot in it.
func (h *Handler) Postgres() *SQLDatabase {
	h.t.Helper()
	return once(h, "postgres", func() *SQLDatabase {
		c := h.start(postgresService)
		h.cleanup("postgres database", func(ctx context.Context) error {
			return dropPostgresDatabase(ctx, c, h.id)
		})
		if _, err := psql(h.t.Context(), c, "postgres", `CREATE DATABASE "`+h.id+`"`); err != nil {
			h.t.Fatalf("testhandler: %s", err)
		}
		return &SQLDatabase{Host: hostIP, Port: c.port, Database: h.id, Username: "postgres", Password: serverKey}
	})
}

// psql runs statements against one database of the server, each in its own transaction.
func psql(ctx context.Context, c *container, database string, statements ...string) (string, error) {
	command := []string{
		"psql", "--host", hostIP, "--username", "postgres", "--dbname", database,
		"--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set", "ON_ERROR_STOP=1",
	}
	for _, statement := range statements {
		command = append(command, "--command", statement)
	}
	return c.exec(ctx, nil, command...)
}

// dropPostgresDatabase drops a database together with its replication slots and publications.
// A slot that is still streaming blocks the drop until its walsender ends, hence the retries.
func dropPostgresDatabase(ctx context.Context, c *container, database string) error {
	var err error
	for range 10 {
		_, err = psql(ctx, c, "postgres",
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE database = '`+database+`' AND active`,
			`DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`,
		)
		if err == nil {
			return nil
		}
		time.Sleep(pollInterval)
	}
	return err
}
