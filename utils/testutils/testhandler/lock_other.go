//go:build !unix

package testhandler

import (
	"context"
	"errors"
)

// lockFile has no implementation off unix, where the handler cannot coordinate test processes.
func lockFile(context.Context, string) (func(), error) {
	return nil, errors.New("testhandler needs a unix host: it coordinates test processes with flock")
}
