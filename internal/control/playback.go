package control

import "github.com/dreylark/heos-control/internal/heos"

// Admission has already authorized replacing any existing playback. Confirm
// Stop first so an old play readback (including the same album) cannot be
// mistaken for confirmation of the new queue. All steps use the usual fresh
// state/wire guards, ownership checks and no-replay command path.
func (c *Coordinator) startPlayback(l *lane, r *execution, cmd Command, item heos.Item) error {
	r.mu.Lock()
	playing := r.expected.State == heos.PlayStatePlay
	r.mu.Unlock()
	if playing {
		if err := c.write(l, r, heos.Mutation{Kind: heos.MutationKindTransport, State: heos.PlayStateStop}); err != nil {
			return err
		}
	}
	steps := []heos.Mutation{
		{Kind: "volume", Level: cmd.Level},
		{Kind: "mode", Repeat: cmd.Repeat, Shuffle: cmd.Shuffle},
		{Kind: "mute", Muted: false},
		{Kind: "queue", Item: item},
	}
	for _, step := range steps {
		if err := c.write(l, r, step); err != nil {
			return err
		}
	}
	r.playbackStarted = c.clock.Now()
	return c.confirmPart(r)
}
