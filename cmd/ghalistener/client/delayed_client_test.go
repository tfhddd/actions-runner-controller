package client

import (
	"context"
	"testing"
	"time"

	"github.com/actions/scaleset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClient is a test double for listener.Client.
type fakeClient struct {
	acquireJobsDelay  time.Duration
	acquireJobsCalled int
	acquiredIDs       []int64
}

func (f *fakeClient) GetMessage(ctx context.Context, lastMessageID, maxCapacity int) (*scaleset.RunnerScaleSetMessage, error) {
	return nil, nil
}

func (f *fakeClient) DeleteMessage(ctx context.Context, messageID int) error {
	return nil
}

func (f *fakeClient) AcquireJobs(ctx context.Context, requestIDs []int64) ([]int64, error) {
	f.acquireJobsCalled++
	return requestIDs, nil
}

func (f *fakeClient) Session() scaleset.RunnerScaleSetSession {
	return scaleset.RunnerScaleSetSession{}
}

func TestDelayedAcquireClient_DelaysAcquireJobs(t *testing.T) {
	delay := 50 * time.Millisecond
	inner := &fakeClient{}
	client := NewDelayedAcquireClient(inner, delay)

	start := time.Now()
	ids, err := client.AcquireJobs(context.Background(), []int64{1, 2, 3})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2, 3}, ids)
	assert.GreaterOrEqual(t, elapsed, delay, "AcquireJobs should be delayed by at least %v", delay)
	assert.Equal(t, 1, inner.acquireJobsCalled)
}

func TestDelayedAcquireClient_CancelDuringDelay(t *testing.T) {
	delay := 500 * time.Millisecond
	inner := &fakeClient{}
	client := NewDelayedAcquireClient(inner, delay)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.AcquireJobs(ctx, []int64{1})
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, delay, "should abort before full delay")
	assert.Equal(t, 0, inner.acquireJobsCalled, "inner AcquireJobs should not be called when ctx cancelled")
}

func TestDelayedAcquireClient_PassThroughGetMessage(t *testing.T) {
	inner := &fakeClient{}
	client := NewDelayedAcquireClient(inner, 10*time.Second)

	start := time.Now()
	msg, err := client.GetMessage(context.Background(), 0, 10)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Nil(t, msg)
	assert.Less(t, elapsed, 100*time.Millisecond, "GetMessage should not be delayed")
}

func TestDelayedAcquireClient_ZeroDelayIsPassThrough(t *testing.T) {
	inner := &fakeClient{}
	client := NewDelayedAcquireClient(inner, 0)

	ids, err := client.AcquireJobs(context.Background(), []int64{7})

	require.NoError(t, err)
	assert.Equal(t, []int64{7}, ids)
	assert.Equal(t, 1, inner.acquireJobsCalled)
}
