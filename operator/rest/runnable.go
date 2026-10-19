package rest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// DefaultShutdownTimeout bounds how long in-flight REST requests may finish
// after the operator is asked to stop.
const DefaultShutdownTimeout = 15 * time.Second

// Runnable runs the mTLS REST server under the controller-runtime manager, so
// it starts and stops with the operator instead of in a detached goroutine.
type Runnable struct {
	Server          *http.Server
	CertFile        string
	KeyFile         string
	ShutdownTimeout time.Duration
}

// Start serves until ctx is cancelled, then shuts down gracefully.
func (r *Runnable) Start(ctx context.Context) error {
	// Tie request contexts to the operator's lifetime, so long-running
	// handlers (a reservation can wait up to 30s for a free cluster) stop
	// when the operator stops instead of outliving Shutdown.
	r.Server.BaseContext = func(net.Listener) context.Context { return ctx }

	errCh := make(chan error, 1)
	go func() {
		err := r.Server.ListenAndServeTLS(r.CertFile, r.KeyFile)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		// Failing to serve (e.g. bad cert, port in use) stops the operator,
		// so the problem is visible instead of silently running without an API.
		return err
	case <-ctx.Done():
	}

	timeout := r.ShutdownTimeout
	if timeout <= 0 {
		timeout = DefaultShutdownTimeout
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := r.Server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-errCh
}

// NeedLeaderElection returns false so every operator replica serves the API.
// Reservations are safe on any replica: they use optimistic locking on the
// Cluster resource.
func (r *Runnable) NeedLeaderElection() bool { return false }
