package testhandler

// Keep third-party imports to testify: go mod tidy follows this file's imports into the go.sum
// of every module whose tests import the package.
import (
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/destination"
	_ "github.com/datazip-inc/olake/destination/parquet"
	"github.com/datazip-inc/olake/utils"
)

func TestNew(t *testing.T) {
	testCases := []struct {
		name  string
		check func(t *testing.T, id string)
	}{
		{
			name: "the id is a lowercase identifier short enough for every system",
			check: func(t *testing.T, id string) {
				require.Regexp(t, `^[a-z][a-z0-9_]{0,29}$`, id)
			},
		},
		{
			name: "the id survives destination name normalization",
			check: func(t *testing.T, id string) {
				require.Equal(t, id, utils.Reformat(id))
			},
		},
		{
			name: "every handler gets its own id",
			check: func(t *testing.T, id string) {
				require.NotEqual(t, id, New(t).ID())
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, New(t).ID())
		})
	}
}

func TestHandlerPostgres(t *testing.T) {
	testCases := []struct {
		name string
		use  func(t *testing.T, database *SQLDatabase)
	}{
		{
			name: "the test's database takes writes and its port answers on the host",
			use: func(t *testing.T, database *SQLDatabase) {
				count, err := psql(t.Context(), postgresService.up, database.Database,
					`CREATE TABLE users (id int PRIMARY KEY)`,
					`INSERT INTO users VALUES (1), (2)`,
					`SELECT count(*) FROM users`)
				require.NoError(t, err)
				require.Equal(t, "2", count)
				conn, err := net.DialTimeout("tcp", net.JoinHostPort(database.Host, strconv.Itoa(database.Port)), 5*time.Second)
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			},
		},
		{
			name: "another test's tables are not visible",
			use: func(t *testing.T, database *SQLDatabase) {
				other := New(t).Postgres()
				_, err := psql(t.Context(), postgresService.up, other.Database, `CREATE TABLE users (id int PRIMARY KEY)`)
				require.NoError(t, err)
				tables, err := psql(t.Context(), postgresService.up, database.Database, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public'`)
				require.NoError(t, err)
				require.Equal(t, "0", tables)
			},
		},
		{
			name: "a replication slot that is still streaming goes with the database",
			use: func(t *testing.T, database *SQLDatabase) {
				slot := database.Database
				_, err := psql(t.Context(), postgresService.up, database.Database,
					`CREATE PUBLICATION `+slot+` FOR ALL TABLES`,
					`SELECT pg_create_logical_replication_slot('`+slot+`', 'pgoutput')`)
				require.NoError(t, err)
				// A walsender that outlives the test, as a leaked CDC connection would.
				_, err = postgresService.up.exec(t.Context(), []string{"--detach"}, "pg_recvlogical", "--username", "postgres",
					"--dbname", database.Database, "--slot", slot, "--start", "--no-loop", "--file", "/dev/null",
					"--option", "proto_version=1", "--option", "publication_names="+slot)
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					active, err := psql(t.Context(), postgresService.up, "postgres", `SELECT active FROM pg_replication_slots WHERE slot_name = '`+slot+`'`)
					return err == nil && active == "t"
				}, 20*time.Second, 200*time.Millisecond, "the slot never started streaming")
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var database *SQLDatabase
			t.Run("test", func(t *testing.T) {
				database = New(t).Postgres()
				tc.use(t, database)
			})
			left, err := psql(t.Context(), postgresService.up, "postgres",
				`SELECT datname FROM pg_database WHERE datname = '`+database.Database+`'
				 UNION ALL SELECT slot_name FROM pg_replication_slots WHERE database = '`+database.Database+`'`)
			require.NoError(t, err)
			require.Empty(t, left, "cleanup must drop the test's database and its replication slots")
		})
	}
}

func TestHandlerMySQL(t *testing.T) {
	testCases := []struct {
		name string
		use  func(t *testing.T, database *SQLDatabase)
	}{
		{
			name: "the test's database takes writes and its port answers on the host",
			use: func(t *testing.T, database *SQLDatabase) {
				table := "`" + database.Database + "`.users"
				count, err := mysql(t.Context(), mysqlService.up,
					"CREATE TABLE "+table+" (id int PRIMARY KEY); INSERT INTO "+table+" VALUES (1), (2); SELECT count(*) FROM "+table)
				require.NoError(t, err)
				require.Equal(t, "2", count)
				conn, err := net.DialTimeout("tcp", net.JoinHostPort(database.Host, strconv.Itoa(database.Port)), 5*time.Second)
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var database *SQLDatabase
			t.Run("test", func(t *testing.T) {
				database = New(t).MySQL()
				tc.use(t, database)
			})
			left, err := mysql(t.Context(), mysqlService.up,
				"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+database.Database+"'")
			require.NoError(t, err)
			require.Empty(t, left, "cleanup must drop the test's database")
		})
	}
}

func TestHandlerMongoDB(t *testing.T) {
	testCases := []struct {
		name string
		use  func(t *testing.T, database *MongoDatabase)
	}{
		{
			name: "the replica set address the test is given serves writes and a change stream",
			use: func(t *testing.T, database *MongoDatabase) {
				// The address is the same inside the container, so its own shell can stand in for a host client.
				uri := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=%s&replicaSet=%s",
					database.Username, database.Password, database.Hosts[0], database.AuthDB, database.ReplicaSet)
				operation, err := mongoService.up.exec(t.Context(), nil, "mongosh", uri, "--quiet", "--eval", fmt.Sprintf(`
					const users = db.getSiblingDB(%q).users
					const changes = users.watch()
					users.insertOne({name: "first"})
					const deadline = Date.now() + 30000
					let change = null
					while ((change = changes.tryNext()) === null && Date.now() < deadline) {}
					print(change === null ? "no change within 30 s" : change.operationType)`, database.Database))
				require.NoError(t, err)
				require.Equal(t, "insert", operation)
				conn, err := net.DialTimeout("tcp", database.Hosts[0], 5*time.Second)
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var database *MongoDatabase
			t.Run("test", func(t *testing.T) {
				database = New(t).MongoDB()
				tc.use(t, database)
			})
			left, err := mongosh(t.Context(), mongoService.up,
				fmt.Sprintf("db.adminCommand({listDatabases: 1, nameOnly: true, filter: {name: %q}}).databases.length", database.Database))
			require.NoError(t, err)
			require.Equal(t, "0", left, "cleanup must drop the test's database")
		})
	}
}

func TestHandlerDestination(t *testing.T) {
	testCases := []struct {
		name string
		kind *DestinationKind
		// prefix is where the kind keeps one test's files in the bucket.
		prefix func(database string) string
		// write creates what the product's writer would create for the test's database.
		write func(t *testing.T, dest *Destination)
		// entries reports what the catalog still holds for the test's database.
		entries func(t *testing.T, database string) string
		// wantFatal is the failure the handler must raise instead of handing out the destination.
		wantFatal string
		// first is a kind the test binary already uses when a wantFatal case asks for its own.
		first *DestinationKind
	}{
		{
			name:   "parquet on minio passes the writer's own check",
			kind:   ParquetMinIO,
			prefix: func(database string) string { return "parquet/" + database },
			write: func(t *testing.T, dest *Destination) {
				_, err := destination.NewWriterPool(t.Context(), dest.Config, nil, 1)
				require.NoError(t, err)
			},
		},
		{
			name:      "a second parquet kind in one test binary fails the test",
			kind:      ParquetLocal,
			wantFatal: "keeps its first config",
			first:     ParquetMinIO,
		},
		{
			name:   "iceberg on a jdbc catalog",
			kind:   IcebergJDBC,
			prefix: func(database string) string { return "iceberg_jdbc/" + database },
			write: func(t *testing.T, dest *Destination) {
				// No writer runs here, so the test creates the catalog's two tables itself, locked against a parallel run.
				_, err := psql(t.Context(), postgresService.up, jdbcCatalogDatabase, `DO $$ BEGIN
					PERFORM pg_advisory_xact_lock(1);
					CREATE TABLE IF NOT EXISTS iceberg_tables (
						catalog_name VARCHAR(255) NOT NULL, table_namespace VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL,
						metadata_location VARCHAR(1000), previous_metadata_location VARCHAR(1000),
						PRIMARY KEY (catalog_name, table_namespace, table_name));
					CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
						catalog_name VARCHAR(255) NOT NULL, namespace VARCHAR(255) NOT NULL, property_key VARCHAR(255) NOT NULL,
						property_value VARCHAR(1000),
						PRIMARY KEY (catalog_name, namespace, property_key));
					INSERT INTO iceberg_tables VALUES ('olake_iceberg', '`+dest.Database+`', 'users', 's3a://`+bucket+`/users', NULL);
					INSERT INTO iceberg_namespace_properties VALUES ('olake_iceberg', '`+dest.Database+`', 'exists', 'true');
				END $$`)
				require.NoError(t, err)
			},
			entries: func(t *testing.T, database string) string {
				rows, err := psql(t.Context(), postgresService.up, jdbcCatalogDatabase,
					`SELECT table_name FROM iceberg_tables WHERE table_namespace = '`+database+`'
					 UNION ALL SELECT property_key FROM iceberg_namespace_properties WHERE namespace = '`+database+`'`)
				require.NoError(t, err)
				return rows
			},
		},
		{
			name:   "iceberg on a rest catalog",
			kind:   IcebergREST,
			prefix: func(database string) string { return "iceberg_rest/" + database },
			write: func(t *testing.T, dest *Destination) {
				catalog := "http://" + restCatalogService.up.addr() + "/v1/namespaces"
				header := map[string]string{"Content-Type": "application/json"}
				status, body, err := request(t.Context(), http.MethodPost, catalog, header, `{"namespace":["`+dest.Database+`"]}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
				// The catalog writes the table's metadata file to MinIO itself.
				status, body, err = request(t.Context(), http.MethodPost, catalog+"/"+dest.Database+"/tables", header,
					`{"name":"users","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"long"}]}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
			},
			entries: func(t *testing.T, database string) string {
				status, body, err := request(t.Context(), http.MethodGet, "http://"+restCatalogService.up.addr()+"/v1/namespaces/"+database, nil, "")
				require.NoError(t, err)
				if status == http.StatusNotFound {
					return ""
				}
				return body
			},
		},
		{
			name:   "iceberg on a hive metastore",
			kind:   IcebergHive,
			prefix: func(database string) string { return "iceberg_hive/" + database + ".db" },
			write: func(t *testing.T, dest *Destination) {
				// The metastore creates the namespace folder in MinIO itself.
				_, err := beeline(t.Context(), hiveMetastoreService.up,
					"CREATE DATABASE "+dest.Database+" LOCATION 's3a://"+bucket+"/iceberg_hive/"+dest.Database+".db'")
				require.NoError(t, err)
			},
			entries: func(t *testing.T, database string) string {
				listed, err := beeline(t.Context(), hiveMetastoreService.up, "SHOW DATABASES")
				require.NoError(t, err)
				require.Contains(t, listed, "default")
				if strings.Contains(listed, database) {
					return listed
				}
				return ""
			},
		},
		{
			name:   "a second handler and a subtest share the test's turn on the iceberg writer",
			kind:   IcebergJDBC,
			prefix: func(database string) string { return "iceberg_jdbc/" + database },
			write: func(t *testing.T, dest *Destination) {
				require.NotEqual(t, dest.Database, New(t).Destination(IcebergGlue).Database)
				t.Run("subtest", func(t *testing.T) {
					require.NotEqual(t, dest.Database, New(t).Destination(IcebergGlue).Database)
				})
			},
		},
		{
			name:   "iceberg on a glue catalog",
			kind:   IcebergGlue,
			prefix: func(database string) string { return "iceberg_glue/" + database + ".db" },
			write: func(t *testing.T, dest *Destination) {
				status, body, err := glueCall(t.Context(), glueService.up, "CreateDatabase", `{"DatabaseInput":{"Name":"`+dest.Database+`"}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
				status, body, err = glueCall(t.Context(), glueService.up, "CreateTable", `{"DatabaseName":"`+dest.Database+`","TableInput":{"Name":"users"}}`)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, status, body)
			},
			entries: func(t *testing.T, database string) string {
				status, body, err := glueCall(t.Context(), glueService.up, "GetDatabase", `{"Name":"`+database+`"}`)
				require.NoError(t, err)
				if status != http.StatusOK && strings.Contains(body, "EntityNotFoundException") {
					return ""
				}
				return body
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantFatal != "" {
				New(t).Destination(tc.first)
				require.Contains(t, fatalMessage(t, func(tb testing.TB) { New(tb).Destination(tc.kind) }), tc.wantFatal)
				return
			}
			var dest *Destination
			t.Run("test", func(t *testing.T) {
				dest = New(t).Destination(tc.kind)
				tc.write(t, dest)
				// Stands in for a data file a writer left under the test's database.
				_, err := mc(t.Context(), minioService.up, "cp", "/etc/hostname", "local/"+bucket+"/"+tc.prefix(dest.Database)+"/users/data/part.parquet")
				require.NoError(t, err)
				require.NotEmpty(t, objects(t, tc.prefix(dest.Database)))
			})
			require.Empty(t, objects(t, tc.prefix(dest.Database)), "cleanup must delete the test's files")
			if tc.entries != nil {
				require.Empty(t, tc.entries(t, dest.Database), "cleanup must drop the test's catalog entries")
			}
		})
	}
}

// objects lists what the shared bucket holds under a key prefix.
func objects(t *testing.T, prefix string) string {
	t.Helper()
	listed, err := mc(t.Context(), minioService.up, "ls", "--recursive", "local/"+bucket+"/"+prefix+"/")
	require.NoError(t, err)
	return listed
}

// fatalRecorder stands in for a test so that a Fatalf can be observed instead of failing the run.
type fatalRecorder struct {
	testing.TB
	message string
}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// fatalMessage runs body against a recorder and returns the Fatalf message it raised, if any.
func fatalMessage(t *testing.T, body func(tb testing.TB)) string {
	t.Helper()
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(recorder)
	}()
	<-done
	return recorder.message
}
