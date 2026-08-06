package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const LogTruncationMarker = "[simpleswe: log truncated at byte quota]\n"

type PodLogCursor struct {
	Timestamp          time.Time
	TimestampOrdinal   int
	UntimestampedLines int
	Exhausted          bool
	Truncated          bool
}

type AppendPodLogParams struct {
	TaskID, AttemptID, PodUID string
	JobName, PodName          string
	Timestamp                 time.Time
	TimestampOrdinal          int
	UntimestampedOrdinal      int
	Content                   []byte
	WorkerEventID             string
	WorkerEvent               string
}

type AppendPodLogResult struct {
	Duplicate     bool
	Truncated     bool
	AppendedBytes int
}

type PendingWorkerEvent struct {
	ID, JobName, PodName, Content string
}

type LogChunk struct {
	Sequence int64
	Content  string
}

type KubernetesJobObservation struct {
	TaskID, AttemptID, APIVersion, Namespace, Name, UID string
	AttemptNumber                                       int
	State, Reason, Message                              string
	StartedAt, CompletedAt                              *time.Time
	SecretName                                          string
}

type KubernetesPodObservation struct {
	TaskID, AttemptID, APIVersion, Namespace, Name, UID  string
	State, Reason, Message, Node, Image, ContainerStates string
	StartedAt, CompletedAt                               *time.Time
}

type SecretCleanup struct {
	TaskID, AttemptID, Namespace, JobName, JobUID, SecretName, SecretUID string
	AttemptNumber                                                        int
	Generation                                                           int64
	EligibleAt                                                           *time.Time
}

func (s *Store) AppendPodLog(ctx context.Context, p AppendPodLogParams, maxBytes, chunkBytes int) (AppendPodLogResult, error) {
	if p.TaskID == "" || p.AttemptID == "" || p.PodUID == "" || chunkBytes <= 0 || maxBytes < len(LogTruncationMarker) {
		return AppendPodLogResult{}, errors.New("valid Pod log identity, chunk size, and byte quota are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AppendPodLogResult{}, fmt.Errorf("begin Pod log append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureAttemptTx(ctx, tx, p.TaskID, p.AttemptID); err != nil {
		return AppendPodLogResult{}, err
	}
	now := stamp(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `INSERT INTO pod_log_state (pod_uid, task_id, attempt_id, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(pod_uid) DO NOTHING`, p.PodUID, p.TaskID, p.AttemptID, now); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("create Pod log cursor: %w", err)
	}
	var taskID, attemptID, lastTimestamp string
	var timestampOrdinal, untimestampedOrdinal int
	if err := tx.QueryRowContext(ctx, `SELECT task_id, attempt_id, last_timestamp, timestamp_ordinal, untimestamped_ordinal FROM pod_log_state WHERE pod_uid = ?`, p.PodUID).Scan(&taskID, &attemptID, &lastTimestamp, &timestampOrdinal, &untimestampedOrdinal); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("read Pod log cursor: %w", err)
	}
	if taskID != p.TaskID || attemptID != p.AttemptID {
		return AppendPodLogResult{}, fmt.Errorf("%w: Pod UID %q belongs to another attempt", ErrConflict, p.PodUID)
	}
	duplicate := false
	if !p.Timestamp.IsZero() {
		if lastTimestamp != "" {
			last, parseErr := parseTime(lastTimestamp)
			if parseErr != nil {
				return AppendPodLogResult{}, fmt.Errorf("parse Pod log cursor: %w", parseErr)
			}
			duplicate = p.Timestamp.Before(last) || (p.Timestamp.Equal(last) && p.TimestampOrdinal <= timestampOrdinal)
		}
	} else {
		duplicate = p.UntimestampedOrdinal > 0 && p.UntimestampedOrdinal <= untimestampedOrdinal
	}
	if duplicate {
		return AppendPodLogResult{Duplicate: true}, tx.Commit()
	}

	var total int
	var truncated int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT total_bytes FROM attempt_log_state WHERE attempt_id = ?), (SELECT COALESCE(SUM(length(content)), 0) FROM log_chunks WHERE attempt_id = ?)), COALESCE((SELECT truncated FROM attempt_log_state WHERE attempt_id = ?), 0)`, p.AttemptID, p.AttemptID, p.AttemptID).Scan(&total, &truncated); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("read attempt log usage: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_log_state (attempt_id, total_bytes, truncated) VALUES (?, ?, ?) ON CONFLICT(attempt_id) DO NOTHING`, p.AttemptID, total, truncated); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("create attempt log state: %w", err)
	}
	result := AppendPodLogResult{Truncated: truncated == 1}
	toAppend := p.Content
	if truncated == 1 {
		toAppend = nil
	} else if total+len(toAppend) > maxBytes {
		keep := maxBytes - len(LogTruncationMarker) - total
		if keep < 0 {
			if err := trimLogTailTx(ctx, tx, p.AttemptID, -keep); err != nil {
				return AppendPodLogResult{}, err
			}
			total += keep
			keep = 0
		}
		if keep > len(toAppend) {
			keep = len(toAppend)
		}
		toAppend = append(append([]byte(nil), toAppend[:keep]...), LogTruncationMarker...)
		truncated, result.Truncated = 1, true
		if err := appendObservationTx(ctx, tx, p.TaskID, p.AttemptID, "log byte quota reached; output truncated", "system", "swe-log-truncated-"+p.AttemptID); err != nil {
			return AppendPodLogResult{}, err
		}
	}
	if len(toAppend) > 0 {
		if err := appendLogBytesTx(ctx, tx, p.TaskID, p.AttemptID, toAppend, chunkBytes); err != nil {
			return AppendPodLogResult{}, err
		}
		result.AppendedBytes = len(toAppend)
		total += len(toAppend)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempt_log_state SET total_bytes = ?, truncated = ? WHERE attempt_id = ?`, total, truncated, p.AttemptID); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("update attempt log state: %w", err)
	}
	last := ""
	if !p.Timestamp.IsZero() {
		last = stamp(p.Timestamp)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pod_log_state SET last_timestamp = CASE WHEN ? = '' THEN last_timestamp ELSE ? END, timestamp_ordinal = CASE WHEN ? = '' THEN timestamp_ordinal ELSE ? END, untimestamped_ordinal = MAX(untimestamped_ordinal, ?), updated_at = ? WHERE pod_uid = ?`, last, last, last, p.TimestampOrdinal, p.UntimestampedOrdinal, now, p.PodUID); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("advance Pod log cursor: %w", err)
	}
	if p.WorkerEventID != "" && p.WorkerEvent != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_log_events (id, pod_uid, task_id, attempt_id, job_name, pod_name, content, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, p.WorkerEventID, p.PodUID, p.TaskID, p.AttemptID, p.JobName, p.PodName, p.WorkerEvent, now); err != nil {
			return AppendPodLogResult{}, fmt.Errorf("queue worker log event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return AppendPodLogResult{}, fmt.Errorf("commit Pod log append: %w", err)
	}
	return result, nil
}

func ensureAttemptTx(ctx context.Context, tx *sql.Tx, taskID, attemptID string) error {
	var found int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM task_attempts WHERE task_id = ? AND id = ?`, taskID, attemptID).Scan(&found); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	} else if err != nil {
		return fmt.Errorf("verify attempt: %w", err)
	}
	return nil
}

func appendLogBytesTx(ctx context.Context, tx *sql.Tx, taskID, attemptID string, content []byte, chunkBytes int) error {
	var sequence int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM log_chunks WHERE attempt_id = ?`, attemptID).Scan(&sequence); err != nil {
		return fmt.Errorf("select log sequence: %w", err)
	}
	for len(content) > 0 {
		size := min(len(content), chunkBytes)
		sequence++
		id, err := newID("swe-log-")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO log_chunks (id, task_id, attempt_id, sequence, content, created_at) VALUES (?, ?, ?, ?, ?, ?)`, id, taskID, attemptID, sequence, content[:size], stamp(time.Now().UTC())); err != nil {
			return fmt.Errorf("append log chunk: %w", err)
		}
		content = content[size:]
	}
	return nil
}

func trimLogTailTx(ctx context.Context, tx *sql.Tx, attemptID string, bytes int) (resultErr error) {
	toTrim := bytes
	rows, err := tx.QueryContext(ctx, `SELECT id, content FROM log_chunks WHERE attempt_id = ? ORDER BY sequence DESC`, attemptID)
	if err != nil {
		return fmt.Errorf("read log tail for truncation: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	type chunk struct {
		id      string
		content []byte
	}
	var chunks []chunk
	for rows.Next() {
		var c chunk
		if err := rows.Scan(&c.id, &c.content); err != nil {
			return err
		}
		chunks = append(chunks, c)
		if toTrim <= len(c.content) {
			break
		}
		toTrim -= len(c.content)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, c := range chunks {
		if bytes >= len(c.content) {
			if _, err := tx.ExecContext(ctx, `DELETE FROM log_chunks WHERE id = ?`, c.id); err != nil {
				return err
			}
			bytes -= len(c.content)
		} else if bytes > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE log_chunks SET content = ? WHERE id = ?`, c.content[:len(c.content)-bytes], c.id); err != nil {
				return err
			}
			bytes = 0
		}
	}
	return nil
}

func appendObservationTx(ctx context.Context, tx *sql.Tx, taskID, attemptID, reason, trigger, id string) error {
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM task_attempts WHERE id = ? AND task_id = ?`, attemptID, taskID).Scan(&state); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO task_events (id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger, resource_identity, metadata, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}', '') ON CONFLICT(id) DO NOTHING`, id, taskID, attemptID, stamp(time.Now().UTC()), state, state, reason, trigger)
	return err
}

func (s *Store) GetPodLogCursor(ctx context.Context, podUID string) (PodLogCursor, error) {
	var result PodLogCursor
	var timestamp string
	var exhausted int
	err := s.db.QueryRowContext(ctx, `SELECT p.last_timestamp, p.timestamp_ordinal, p.untimestamped_ordinal, p.exhausted, COALESCE(a.truncated, 0) FROM pod_log_state p LEFT JOIN attempt_log_state a ON a.attempt_id = p.attempt_id WHERE p.pod_uid = ?`, podUID).Scan(&timestamp, &result.TimestampOrdinal, &result.UntimestampedLines, &exhausted, &result.Truncated)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("get Pod log cursor: %w", err)
	}
	if timestamp != "" {
		result.Timestamp, err = parseTime(timestamp)
		if err != nil {
			return PodLogCursor{}, err
		}
	}
	result.Exhausted = exhausted == 1
	return result, nil
}

func (s *Store) ListPendingWorkerEvents(ctx context.Context, podUID string) (_ []PendingWorkerEvent, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, job_name, pod_name, content FROM worker_log_events WHERE pod_uid = ? AND processed = 0 ORDER BY rowid`, podUID)
	if err != nil {
		return nil, fmt.Errorf("list pending worker events: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var result []PendingWorkerEvent
	for rows.Next() {
		var event PendingWorkerEvent
		if err := rows.Scan(&event.ID, &event.JobName, &event.PodName, &event.Content); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *Store) MarkWorkerEventProcessed(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE worker_log_events SET processed = 1 WHERE id = ?`, id)
	return err
}

func (s *Store) MarkPodLogsExhausted(ctx context.Context, podUID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE pod_log_state SET exhausted = 1, updated_at = ? WHERE pod_uid = ?`, stamp(time.Now().UTC()), podUID)
	return err
}

func (s *Store) ReadLogTailCursor(ctx context.Context, taskID, attemptID string, lines int) (_ string, _ int64, resultErr error) {
	if lines < 0 {
		return "", 0, errors.New("log tail lines must not be negative")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var cursor int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM log_chunks WHERE task_id = ? AND attempt_id = ?`, taskID, attemptID).Scan(&cursor); err != nil {
		return "", 0, err
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM task_attempts WHERE task_id = ? AND id = ?`, taskID, attemptID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return "", 0, fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	} else if err != nil {
		return "", 0, err
	}
	if lines == 0 {
		if err := tx.Commit(); err != nil {
			return "", 0, err
		}
		return "", cursor, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT content FROM log_chunks WHERE task_id = ? AND attempt_id = ? AND sequence <= ? ORDER BY sequence DESC`, taskID, attemptID, cursor)
	if err != nil {
		return "", 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var reversed [][]byte
	newlines, required := 0, lines
	first := true
	for rows.Next() {
		var chunk []byte
		if err := rows.Scan(&chunk); err != nil {
			return "", 0, err
		}
		if first {
			first = false
			if len(chunk) > 0 && chunk[len(chunk)-1] == '\n' {
				required++
			}
		}
		reversed = append(reversed, append([]byte(nil), chunk...))
		newlines += strings.Count(string(chunk), "\n")
		if newlines >= required {
			break
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return "", 0, err
	}
	var content strings.Builder
	for i := len(reversed) - 1; i >= 0; i-- {
		content.Write(reversed[i])
	}
	if err := tx.Commit(); err != nil {
		return "", 0, err
	}
	return lastLines(content.String(), lines), cursor, nil
}

func (s *Store) ReadLogChunksAfter(ctx context.Context, taskID, attemptID string, sequence int64, limit int) (_ []LogChunk, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, CAST(content AS TEXT) FROM log_chunks WHERE task_id = ? AND attempt_id = ? AND sequence > ? ORDER BY sequence LIMIT ?`, taskID, attemptID, sequence, limit)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var result []LogChunk
	for rows.Next() {
		var chunk LogChunk
		if err := rows.Scan(&chunk.Sequence, &chunk.Content); err != nil {
			return nil, err
		}
		result = append(result, chunk)
	}
	return result, rows.Err()
}

func (s *Store) AttemptFollowComplete(ctx context.Context, taskID, attemptID string) (bool, error) {
	attempt, err := s.GetAttempt(ctx, taskID, attemptID)
	if err != nil {
		return false, err
	}
	return attempt.LogsExhausted, nil
}

func (s *Store) ObserveKubernetesJob(ctx context.Context, o KubernetesJobObservation) error {
	if o.TaskID == "" || o.AttemptID == "" || o.Namespace == "" || o.Name == "" || o.UID == "" {
		return errors.New("complete Job observation identity is required")
	}
	if o.APIVersion == "" {
		o.APIVersion = "batch/v1"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Job observation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureAttemptTx(ctx, tx, o.TaskID, o.AttemptID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO kubernetes_jobs (attempt_id, task_id, api_version, namespace, name, uid, state, reason, message, started_at, completed_at, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(attempt_id) DO UPDATE SET api_version=excluded.api_version, namespace=excluded.namespace, name=excluded.name, uid=excluded.uid, state=excluded.state, reason=excluded.reason, message=excluded.message, started_at=excluded.started_at, completed_at=excluded.completed_at, observed_at=excluded.observed_at`, o.AttemptID, o.TaskID, o.APIVersion, o.Namespace, o.Name, o.UID, o.State, o.Reason, o.Message, nullableTime(o.StartedAt), nullableTime(o.CompletedAt), stamp(time.Now().UTC()))
	if err != nil {
		return fmt.Errorf("observe Kubernetes Job: %w", err)
	}
	if o.SecretName != "" {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO secret_cleanups (attempt_id, attempt_number, task_id, namespace, job_name, job_uid, secret_name)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(attempt_id) DO UPDATE SET
				attempt_number=excluded.attempt_number,
				generation=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.job_uid <> excluded.job_uid OR secret_cleanups.secret_name <> excluded.secret_name THEN secret_cleanups.generation + 1 ELSE secret_cleanups.generation END,
				secret_uid=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.job_uid <> excluded.job_uid OR secret_cleanups.secret_name <> excluded.secret_name THEN '' ELSE secret_cleanups.secret_uid END,
				eligible_at=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.job_uid <> excluded.job_uid OR secret_cleanups.secret_name <> excluded.secret_name THEN NULL ELSE secret_cleanups.eligible_at END,
				completed_at=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.job_uid <> excluded.job_uid OR secret_cleanups.secret_name <> excluded.secret_name THEN NULL ELSE secret_cleanups.completed_at END,
				namespace=excluded.namespace, job_name=excluded.job_name, job_uid=excluded.job_uid, secret_name=excluded.secret_name`,
			o.AttemptID, o.AttemptNumber, o.TaskID, o.Namespace, o.Name, o.UID, o.SecretName)
	}
	if err != nil {
		return fmt.Errorf("record Job Secret cleanup: %w", err)
	}
	return tx.Commit()
}

func (s *Store) ObserveKubernetesPod(ctx context.Context, o KubernetesPodObservation) error {
	if o.TaskID == "" || o.AttemptID == "" || o.Namespace == "" || o.Name == "" || o.UID == "" {
		return errors.New("complete Pod observation identity is required")
	}
	if o.APIVersion == "" {
		o.APIVersion = "v1"
	}
	if o.ContainerStates == "" {
		o.ContainerStates = "{}"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureAttemptTx(ctx, tx, o.TaskID, o.AttemptID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kubernetes_pods (uid, attempt_id, task_id, api_version, namespace, name, state, reason, message, node, image, container_states, started_at, completed_at, observed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(uid) DO UPDATE SET state=excluded.state, reason=excluded.reason, message=excluded.message, node=excluded.node, image=excluded.image, container_states=excluded.container_states, started_at=excluded.started_at, completed_at=excluded.completed_at, observed_at=excluded.observed_at`, o.UID, o.AttemptID, o.TaskID, o.APIVersion, o.Namespace, o.Name, o.State, o.Reason, o.Message, o.Node, o.Image, o.ContainerStates, nullableTime(o.StartedAt), nullableTime(o.CompletedAt), stamp(time.Now().UTC())); err != nil {
		return fmt.Errorf("observe Kubernetes Pod: %w", err)
	}
	return tx.Commit()
}

func (s *Store) AttemptKubernetes(ctx context.Context, attemptID string) (KubernetesJobObservation, KubernetesPodObservation, error) {
	var job KubernetesJobObservation
	var js, jc sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT task_id, attempt_id, api_version, namespace, name, uid, state, reason, message, started_at, completed_at FROM kubernetes_jobs WHERE attempt_id = ?`, attemptID).Scan(&job.TaskID, &job.AttemptID, &job.APIVersion, &job.Namespace, &job.Name, &job.UID, &job.State, &job.Reason, &job.Message, &js, &jc)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return job, KubernetesPodObservation{}, err
	}
	parseNullableTimes(js, jc, &job.StartedAt, &job.CompletedAt)
	var pod KubernetesPodObservation
	var ps, pc sql.NullString
	err = s.db.QueryRowContext(ctx, `SELECT task_id, attempt_id, api_version, namespace, name, uid, state, reason, message, node, image, container_states, started_at, completed_at FROM kubernetes_pods WHERE attempt_id = ? ORDER BY observed_at DESC, rowid DESC LIMIT 1`, attemptID).Scan(&pod.TaskID, &pod.AttemptID, &pod.APIVersion, &pod.Namespace, &pod.Name, &pod.UID, &pod.State, &pod.Reason, &pod.Message, &pod.Node, &pod.Image, &pod.ContainerStates, &ps, &pc)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return job, pod, err
	}
	parseNullableTimes(ps, pc, &pod.StartedAt, &pod.CompletedAt)
	return job, pod, nil
}

func nullableTime(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return stamp(*value)
}
func parseNullableTimes(a, b sql.NullString, first, second **time.Time) {
	if a.Valid {
		if value, err := parseTime(a.String); err == nil {
			*first = &value
		}
	}
	if b.Valid {
		if value, err := parseTime(b.String); err == nil {
			*second = &value
		}
	}
}

func (s *Store) MarkSecretCleanupEligible(ctx context.Context, cleanup SecretCleanup, at time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE secret_cleanups SET eligible_at = COALESCE(eligible_at, ?) WHERE attempt_id = ? AND generation = ? AND job_uid = ? AND completed_at IS NULL`, stamp(at), cleanup.AttemptID, cleanup.Generation, cleanup.JobUID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: stale Secret cleanup generation for attempt %s", ErrConflict, cleanup.AttemptID)
	}
	return nil
}

func (s *Store) MarkSecretCleanupIneligible(ctx context.Context, attemptID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE secret_cleanups SET eligible_at = NULL, secret_uid = '', generation = generation + 1 WHERE attempt_id = ? AND completed_at IS NULL AND eligible_at IS NOT NULL`, attemptID)
	return err
}

// RegisterSecretCleanup records ownership of the task-specific Secret before
// Job creation, so a Secret-only attempt boundary remains recoverable.
func (s *Store) RegisterSecretCleanup(ctx context.Context, cleanup SecretCleanup) error {
	if cleanup.TaskID == "" || cleanup.AttemptID == "" || cleanup.AttemptNumber <= 0 || cleanup.Namespace == "" || cleanup.JobName == "" || cleanup.SecretName == "" {
		return errors.New("complete task Secret cleanup identity is required")
	}
	if err := ensureAttempt(ctx, s.db, cleanup.TaskID, cleanup.AttemptID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO secret_cleanups (attempt_id, attempt_number, task_id, namespace, job_name, job_uid, secret_name)
		VALUES (?, ?, ?, ?, ?, '', ?)
		ON CONFLICT(attempt_id) DO UPDATE SET
			attempt_number=excluded.attempt_number, task_id=excluded.task_id,
			generation=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.secret_name <> excluded.secret_name THEN secret_cleanups.generation + 1 ELSE secret_cleanups.generation END,
			secret_uid=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.secret_name <> excluded.secret_name THEN '' ELSE secret_cleanups.secret_uid END,
			eligible_at=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.secret_name <> excluded.secret_name THEN NULL ELSE secret_cleanups.eligible_at END,
			completed_at=CASE WHEN secret_cleanups.namespace <> excluded.namespace OR secret_cleanups.job_name <> excluded.job_name OR secret_cleanups.secret_name <> excluded.secret_name THEN NULL ELSE secret_cleanups.completed_at END,
			namespace=excluded.namespace, job_name=excluded.job_name, secret_name=excluded.secret_name`,
		cleanup.AttemptID, cleanup.AttemptNumber, cleanup.TaskID, cleanup.Namespace, cleanup.JobName, cleanup.SecretName)
	if err != nil {
		return fmt.Errorf("register task Secret cleanup: %w", err)
	}
	return nil
}

func ensureAttempt(ctx context.Context, db *sql.DB, taskID, attemptID string) error {
	var found int
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM task_attempts WHERE task_id = ? AND id = ?`, taskID, attemptID).Scan(&found); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	} else if err != nil {
		return fmt.Errorf("verify attempt: %w", err)
	}
	return nil
}
func (s *Store) BindSecretCleanupUID(ctx context.Context, cleanup SecretCleanup, secretUID string) (SecretCleanup, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE secret_cleanups SET generation = CASE WHEN secret_uid <> '' AND secret_uid <> ? THEN generation + 1 ELSE generation END, secret_uid = ? WHERE attempt_id = ? AND generation = ? AND job_uid = ? AND secret_name = ? AND completed_at IS NULL AND eligible_at IS NOT NULL`, secretUID, secretUID, cleanup.AttemptID, cleanup.Generation, cleanup.JobUID, cleanup.SecretName)
	if err != nil {
		return SecretCleanup{}, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return SecretCleanup{}, fmt.Errorf("%w: stale Secret identity for attempt %s", ErrConflict, cleanup.AttemptID)
	}
	return s.GetSecretCleanup(ctx, cleanup.AttemptID)
}

func (s *Store) CompleteSecretCleanup(ctx context.Context, cleanup SecretCleanup) error {
	result, err := s.db.ExecContext(ctx, `UPDATE secret_cleanups SET completed_at = ? WHERE attempt_id = ? AND generation = ? AND job_uid = ? AND secret_name = ? AND secret_uid = ? AND completed_at IS NULL`, stamp(time.Now().UTC()), cleanup.AttemptID, cleanup.Generation, cleanup.JobUID, cleanup.SecretName, cleanup.SecretUID)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: stale Secret cleanup completion for attempt %s", ErrConflict, cleanup.AttemptID)
	}
	return nil
}
func (s *Store) GetSecretCleanup(ctx context.Context, attemptID string) (SecretCleanup, error) {
	var cleanup SecretCleanup
	var eligible sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT task_id, attempt_id, attempt_number, namespace, job_name, job_uid, secret_name, secret_uid, generation, eligible_at FROM secret_cleanups WHERE attempt_id = ? AND completed_at IS NULL`, attemptID).Scan(
		&cleanup.TaskID, &cleanup.AttemptID, &cleanup.AttemptNumber, &cleanup.Namespace, &cleanup.JobName, &cleanup.JobUID, &cleanup.SecretName, &cleanup.SecretUID, &cleanup.Generation, &eligible,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return cleanup, fmt.Errorf("%w: Secret cleanup for attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return cleanup, err
	}
	if eligible.Valid {
		value, err := parseTime(eligible.String)
		if err != nil {
			return cleanup, err
		}
		cleanup.EligibleAt = &value
	}
	return cleanup, nil
}
func (s *Store) ListSecretCleanups(ctx context.Context) (_ []SecretCleanup, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `SELECT task_id, attempt_id, attempt_number, namespace, job_name, job_uid, secret_name, secret_uid, generation, eligible_at FROM secret_cleanups WHERE completed_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var result []SecretCleanup
	for rows.Next() {
		var c SecretCleanup
		var at sql.NullString
		if err := rows.Scan(&c.TaskID, &c.AttemptID, &c.AttemptNumber, &c.Namespace, &c.JobName, &c.JobUID, &c.SecretName, &c.SecretUID, &c.Generation, &at); err != nil {
			return nil, err
		}
		if at.Valid {
			value, _ := parseTime(at.String)
			c.EligibleAt = &value
		}
		result = append(result, c)
	}
	return result, rows.Err()
}
