package testhandler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	hostIP          = "127.0.0.1"
	networkName     = "olake-testhandler"
	managedLabel    = "olake.testhandler"
	portLabel       = "olake.testhandler.port"
	portPlaceholder = "{port}"
	pollInterval    = 250 * time.Millisecond
	commandTimeout  = 2 * time.Minute
	pullTimeout     = 20 * time.Minute
	// The Iceberg writer listens here and kills any other listener, so no container may publish it.
	icebergWriterPort = 50051
)

var errPortTaken = errors.New("host port taken between choosing and binding it")

// service is the recipe for one shared container. Its image tag and server flags live only here.
type service struct {
	name  string
	image string
	// port is the container port published on the host. Zero uses the host port inside too.
	port       int
	env        []string
	entrypoint string
	// args replace the image command; portPlaceholder becomes the host port.
	args    []string
	needs   []*service
	startup time.Duration
	// ready converges the service and reports readiness. Many processes may run it at once.
	ready func(ctx context.Context, c *container) error

	mu sync.Mutex
	up *container
}

// container is a running service as this process reaches it.
type container struct {
	name string
	port int
}

func (c *container) addr() string {
	return net.JoinHostPort(hostIP, strconv.Itoa(c.port))
}

// exec runs a command inside the container; flags are extra `docker exec` flags.
func (c *container) exec(ctx context.Context, flags []string, command ...string) (string, error) {
	args := append([]string{"exec"}, flags...)
	return docker(ctx, append(append(args, c.name), command...)...)
}

// docker runs one docker CLI command and returns its trimmed stdout.
func docker(ctx context.Context, args ...string) (string, error) {
	return dockerWithin(ctx, commandTimeout, args...)
}

// dockerWithin bounds the command, so a daemon that stalls fails the call instead of hanging the test.
func dockerWithin(ctx context.Context, limit time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()+stdout.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// dockerReady checks once per process that the docker CLI exists and the daemon answers.
var dockerReady = sync.OnceValue(func() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("this test needs Docker and the docker CLI is not on PATH: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := docker(ctx, "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("this test needs Docker and the daemon is not answering: %w", err)
	}
	return nil
})

// containerName carries a hash of the recipe, so a changed tag or flag never adopts an old container.
func (s *service) containerName() string {
	recipe := append([]string{s.image, strconv.Itoa(s.port), s.entrypoint}, s.env...)
	sum := sha256.Sum256([]byte(strings.Join(append(recipe, s.args...), "\x00")))
	return "olake-testhandler-" + s.name + "-" + hex.EncodeToString(sum[:4])
}

// ensure returns the service's ready container: started here, adopted from another process, or restarted.
func (s *service) ensure(ctx context.Context, t testing.TB) (*container, error) {
	if err := dockerReady(); err != nil {
		return nil, err
	}
	for _, need := range s.needs {
		if _, err := need.ensure(ctx, t); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.up != nil {
		return s.up, nil
	}

	name := s.containerName()
	deadline := time.Now().Add(s.startup)
	restarted := false
	var probeErr error
	for {
		// The inspect is also the liveness check: it fails once the daemon stops answering or stalls.
		status, port, err := inspect(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("waiting for container %s (%s): %w", name, s.image, err)
		}
		switch status {
		case "":
			if err := s.create(ctx, t, name); err != nil && !errors.Is(err, errPortTaken) {
				return nil, err
			}
			deadline = time.Now().Add(s.startup)
		case "running":
			c := &container{name: name, port: port}
			if probeErr = s.ready(ctx, c); probeErr == nil {
				s.up = c
				return c, nil
			}
		case "created", "exited":
			if restarted {
				return nil, fmt.Errorf("container %s (%s) stopped while starting; last log lines:\n%s", name, s.image, logs(name))
			}
			restarted = status == "exited"
			if _, probeErr = docker(ctx, "start", name); probeErr != nil {
				// Its host port may be taken by now, so it is removed for the next round to recreate; a racing process may win.
				_, _ = docker(ctx, "rm", name)
				restarted = false
			}
		case "removing", "restarting":
			// In transition, possibly by another process: the next inspect decides.
		default:
			return nil, fmt.Errorf("container %s is %s; remove it with: docker rm -f %s", name, status, name)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("container %s (%s) not ready after %s: %v; last log lines:\n%s", name, s.image, s.startup, probeErr, logs(name))
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for container %s (%s): %w", name, s.image, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

// create pulls the image if needed and runs the container, unless another process got there first.
func (s *service) create(ctx context.Context, t testing.TB, name string) error {
	if _, err := docker(ctx, "image", "inspect", "--format", "{{.Id}}", s.image); err != nil {
		t.Logf("testhandler: pulling %s, first use on this machine", s.image)
		if _, err := dockerWithin(ctx, pullTimeout, "pull", "--quiet", s.image); err != nil {
			return err
		}
	}
	// Docker does not keep network names unique under concurrent creates, so creation is serialized.
	unlock, err := lockFile(ctx, filepath.Join(os.TempDir(), "olake-testhandler-docker.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	if status, _, err := inspect(ctx, name); err != nil || status != "" {
		return err
	}
	if _, err := docker(ctx, "network", "inspect", "--format", "{{.Id}}", networkName); err != nil {
		if _, err := docker(ctx, "network", "create", "--label", managedLabel+"=true", networkName); err != nil {
			return err
		}
	}
	port, err := freePort()
	if err != nil {
		return err
	}
	_, err = docker(ctx, s.runArgs(name, port)...)
	if err == nil {
		t.Logf("testhandler: started container %s (%s) on %s:%d", name, s.image, hostIP, port)
		return nil
	}
	// A failed run leaves the container behind in state created.
	_, _ = docker(ctx, "rm", name)
	if strings.Contains(err.Error(), "port is already allocated") || strings.Contains(err.Error(), "address already in use") {
		return errPortTaken
	}
	return err
}

// runArgs is the `docker run` argument list for this service on the given host port.
func (s *service) runArgs(name string, port int) []string {
	containerPort := s.port
	if containerPort == 0 {
		containerPort = port
	}
	args := []string{
		"run", "--detach", "--name", name, "--network", networkName,
		"--label", managedLabel + "=true", "--label", portLabel + "=" + strconv.Itoa(port),
		"--publish", fmt.Sprintf("%s:%d:%d", hostIP, port, containerPort),
	}
	for _, env := range s.env {
		args = append(args, "--env", env)
	}
	if s.entrypoint != "" {
		args = append(args, "--entrypoint", s.entrypoint)
	}
	args = append(args, s.image)
	for _, arg := range s.args {
		args = append(args, strings.ReplaceAll(arg, portPlaceholder, strconv.Itoa(port)))
	}
	return args
}

// inspect reports a container's state and published host port. An absent container has an empty state.
func inspect(ctx context.Context, name string) (status string, port int, err error) {
	out, err := docker(ctx, "container", "inspect", "--format", `{{.State.Status}} {{index .Config.Labels "`+portLabel+`"}}`, name)
	if err != nil {
		if strings.Contains(err.Error(), "No such container") {
			return "", 0, nil
		}
		return "", 0, err
	}
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return "", 0, fmt.Errorf("container %s was not started by testhandler: inspect returned %q", name, out)
	}
	port, err = strconv.Atoi(fields[1])
	return fields[0], port, err
}

// freePort asks the OS for a free loopback port and never returns the Iceberg writer's port.
func freePort() (int, error) {
	for {
		listener, err := net.Listen("tcp", net.JoinHostPort(hostIP, "0"))
		if err != nil {
			return 0, err
		}
		_, portText, _ := net.SplitHostPort(listener.Addr().String())
		_ = listener.Close()
		port, err := strconv.Atoi(portText)
		if err != nil {
			return 0, err
		}
		if port != icebergWriterPort {
			return port, nil
		}
	}
}

// logs returns the container's last log lines for a failure message.
func logs(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "logs", "--tail", "40", name).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(no logs: %s)", err)
	}
	return strings.TrimSpace(string(out))
}
