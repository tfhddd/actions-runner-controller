package client

import (
	"context"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

// DelayedAcquireClient wraps a listener.Client and adds a delay before AcquireJobs.
// Deploy this on the fallback cluster so the primary cluster always wins the race
// to acquire jobs from GitHub when it has available capacity.
type DelayedAcquireClient struct {
	inner listener.Client
	delay time.Duration
}

var _ listener.Client = (*DelayedAcquireClient)(nil)

func NewDelayedAcquireClient(inner listener.Client, delay time.Duration) *DelayedAcquireClient {
	return &DelayedAcquireClient{inner: inner, delay: delay}
}

func (c *DelayedAcquireClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*scaleset.RunnerScaleSetMessage, error) {
	return c.inner.GetMessage(ctx, lastMessageID, maxCapacity)
}

func (c *DelayedAcquireClient) DeleteMessage(ctx context.Context, messageID int) error {
	return c.inner.DeleteMessage(ctx, messageID)
}

// AcquireJobs waits for the configured delay before forwarding the call to GitHub.
// If ctx is cancelled during the wait (e.g. shutdown), it returns immediately
// without acquiring, so no job is locked to this cluster.
func (c *DelayedAcquireClient) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.inner.AcquireJobs(ctx, requestIDs)
}

func (c *DelayedAcquireClient) Session() scaleset.RunnerScaleSetSession {
	return c.inner.Session()
}
