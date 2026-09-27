package store

// SetBeforeCommit installs a hook that runs before each group commit.
// Set it before the first Store.
func (p *Pebble) SetBeforeCommit(f func(writes int)) { p.beforeCommit = f }

// PendingWrites reports how many writes wait in the forming group.
func (p *Pebble) PendingWrites() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pending == nil {
		return 0
	}
	return int(p.pending.batch.Count())
}
