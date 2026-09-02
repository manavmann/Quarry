package store

import "context"

// StateCounts reads a single SQLite snapshot for startup diagnostics.
func (s *Store) StateCounts(ctx context.Context) (map[string]map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT 'runs', state, COUNT(*) FROM runs GROUP BY state
		UNION ALL SELECT 'jobs', state, COUNT(*) FROM jobs GROUP BY state
		UNION ALL SELECT 'runners', state, COUNT(*) FROM runners GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]map[string]int64{"runs": {}, "jobs": {}, "runners": {}}
	for rows.Next() {
		var kind, state string
		var n int64
		if err := rows.Scan(&kind, &state, &n); err != nil {
			return nil, err
		}
		counts[kind][state] = n
	}
	return counts, rows.Err()
}
