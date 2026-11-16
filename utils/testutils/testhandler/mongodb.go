package testhandler

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

const mongoReplicaSet = "rs0"

// MongoDatabase is one test's own database on the shared replica set, addressed as a client on the host sees it.
type MongoDatabase struct {
	Hosts      []string
	ReplicaSet string
	AuthDB     string
	Database   string
	Username   string
	Password   string
}

// The replica set advertises its member address to clients, so mongod listens on the host port
// inside the container too: 127.0.0.1:<port> is then the same server from both sides.
var mongoService = &service{
	name:    "mongodb",
	image:   "mongo:9.0",
	args:    []string{"mongod", "--replSet", mongoReplicaSet, "--bind_ip_all", "--port", portPlaceholder},
	startup: 2 * time.Minute,
	// Clients must log in even though access control is off, so the probe also creates the user.
	ready: func(ctx context.Context, c *container) error {
		_, err := mongosh(ctx, c, fmt.Sprintf(`
			try { rs.status() } catch (e) { rs.initiate({_id: %q, members: [{_id: 0, host: %q}]}) }
			if (!db.hello().isWritablePrimary) { throw new Error("no primary yet") }
			const admin = db.getSiblingDB("admin")
			if (admin.getUser(%q) === null) { admin.createUser({user: %q, pwd: %q, roles: ["root"]}) }`,
			mongoReplicaSet, c.addr(), serverUser, serverUser, serverKey))
		return err
	},
}

// MongoDB returns this test's own database on the shared single-node replica set, so change streams work.
// MongoDB creates the database on first write. Cleanup drops it.
func (h *Handler) MongoDB() *MongoDatabase {
	h.t.Helper()
	return once(h, "mongodb", func() *MongoDatabase {
		c := h.start(mongoService)
		h.cleanup("mongodb database", func(ctx context.Context) error {
			_, err := mongosh(ctx, c, fmt.Sprintf("db.getSiblingDB(%q).dropDatabase()", h.id))
			return err
		})
		return &MongoDatabase{
			Hosts:      []string{c.addr()},
			ReplicaSet: mongoReplicaSet,
			AuthDB:     "admin",
			Database:   h.id,
			Username:   serverUser,
			Password:   serverKey,
		}
	})
}

// mongosh runs a script on the server; a thrown error fails the command.
func mongosh(ctx context.Context, c *container, script string) (string, error) {
	return c.exec(ctx, nil, "mongosh", "--port", strconv.Itoa(c.port), "--quiet", "--eval", script)
}
