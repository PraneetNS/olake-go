package testhandler

import (
	"context"
	"time"
)

var mysqlService = &service{
	name:  "mysql",
	image: "mysql:8.0",
	port:  3306,
	env:   []string{"MYSQL_ROOT_PASSWORD=" + serverKey},
	args: []string{
		"--server-id=1",
		"--log-bin=mysql-bin",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
		"--binlog_row_metadata=FULL",
		"--default-authentication-plugin=mysql_native_password",
	},
	startup: 3 * time.Minute,
	// The probe uses TCP: the image's init-time server has networking off and then restarts.
	ready: func(ctx context.Context, c *container) error {
		_, err := mysql(ctx, c, "SELECT 1")
		return err
	},
}

// MySQL returns this test's own database on the shared MySQL server, which writes a row-format binlog.
// Cleanup drops the database with everything in it.
func (h *Handler) MySQL() *SQLDatabase {
	h.t.Helper()
	return once(h, "mysql", func() *SQLDatabase {
		c := h.start(mysqlService)
		h.cleanup("mysql database", func(ctx context.Context) error {
			// The timeout turns a drop blocked by a leaked transaction into an error instead of a hang.
			_, err := mysql(ctx, c, "SET SESSION lock_wait_timeout = 30; DROP DATABASE IF EXISTS `"+h.id+"`")
			return err
		})
		if _, err := mysql(h.t.Context(), c, "CREATE DATABASE `"+h.id+"`"); err != nil {
			h.t.Fatalf("testhandler: %s", err)
		}
		return &SQLDatabase{Host: hostIP, Port: c.port, Database: h.id, Username: "root", Password: serverKey}
	})
}

// mysql runs statements on the server as root.
func mysql(ctx context.Context, c *container, statements string) (string, error) {
	return c.exec(ctx, []string{"--env", "MYSQL_PWD=" + serverKey},
		"mysql", "--host", hostIP, "--user", "root", "--skip-column-names", "--execute", statements)
}
