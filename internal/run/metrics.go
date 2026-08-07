package run

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

type metricsHandler struct{ store *store.Store }

func (h metricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	tasks, err := h.store.ListTasks(r.Context())
	if err != nil {
		http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	states := make(map[string]int)
	completed, failed, cancelled, active, attempts := 0, 0, 0, 0, 0
	var durationSum float64
	for _, record := range tasks {
		states[string(record.State)]++
		switch record.State {
		case task.READY:
			completed++
			durationSum += record.UpdatedAt.Sub(record.CreatedAt).Seconds()
		case task.FAILED:
			failed++
			durationSum += record.UpdatedAt.Sub(record.CreatedAt).Seconds()
		case task.CANCELLED:
			cancelled++
			durationSum += record.UpdatedAt.Sub(record.CreatedAt).Seconds()
		default:
			active++
		}
		listed, listErr := h.store.ListAttempts(r.Context(), record.ID)
		if listErr != nil {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		attempts += len(listed)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	write := func(format string, values ...any) bool {
		_, err := fmt.Fprintf(w, format, values...)
		return err == nil
	}
	if !write("# HELP simpleswe_tasks_total Durable tasks by internal state.\n") || !write("# TYPE simpleswe_tasks_total gauge\n") {
		return
	}
	keys := make([]string, 0, len(states))
	for state := range states {
		keys = append(keys, state)
	}
	sort.Strings(keys)
	for _, state := range keys {
		if !write("simpleswe_tasks_total{state=%q} %d\n", state, states[state]) {
			return
		}
	}
	values := map[string]float64{
		"simpleswe_tasks_created_total": float64(len(tasks)), "simpleswe_tasks_completed_total": float64(completed),
		"simpleswe_tasks_failed_total": float64(failed), "simpleswe_tasks_cancelled_total": float64(cancelled),
		"simpleswe_active_tasks": float64(active), "simpleswe_task_duration_seconds": durationSum,
		"simpleswe_jobs_created_total": float64(attempts), "simpleswe_worker_jobs_active": float64(active),
		"simpleswe_worker_job_failures_total": float64(failed),
	}
	for _, name := range []string{"simpleswe_job_creation_failures_total", "simpleswe_reconcile_total", "simpleswe_reconcile_errors_total", "simpleswe_reconciliation_errors_total"} {
		values[name] = 0
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !write("# TYPE %s gauge\n%s %g\n", name, name, values[name]) {
			return
		}
	}
}
