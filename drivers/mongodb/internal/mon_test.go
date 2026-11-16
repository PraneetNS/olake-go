package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestMongoGetStreamNames(t *testing.T) {
	testCases := []struct {
		name        string
		collections []string
		want        []string
	}{
		{
			name:        "lists the collections of the configured database",
			collections: []string{"users", "orders"},
			want:        []string{"users", "orders"},
		},
		{
			name: "a database without collections has no streams",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			database := testhandler.New(t).MongoDB()
			m := &Mongo{config: &Config{
				Hosts:      database.Hosts,
				ReplicaSet: database.ReplicaSet,
				AuthDB:     database.AuthDB,
				Database:   database.Database,
				Username:   database.Username,
				Password:   database.Password,
			}}
			require.NoError(t, m.Setup(t.Context()))
			t.Cleanup(func() { _ = m.Close(context.Background()) })
			for _, collection := range tc.collections {
				require.NoError(t, m.client.Database(database.Database).CreateCollection(t.Context(), collection))
			}

			got, err := m.GetStreamNames(t.Context())
			require.NoError(t, err)
			want := make([]types.StreamID, 0, len(tc.want))
			for _, name := range tc.want {
				want = append(want, types.StreamID{Namespace: database.Database, Name: name})
			}
			require.ElementsMatch(t, want, got)
		})
	}
}
