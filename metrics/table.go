package metrics

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	tableMetrics = map[string]string{
		"seq_scan":          "Number of sequential scans initiated on this table",
		"seq_tup_read":      "Number of live rows fetched by sequential scans",
		"vacuum_count":      "Number of times this table has been manually vacuumed (not counting VACUUM FULL)",
		"autovacuum_count":  "Number of times this table has been vacuumed by the autovacuum daemon",
		"analyze_count":     "Number of times this table has been manually analyzed",
		"autoanalyze_count": "Number of times this table has been analyzed by the autovacuum daemon",
		"n_tup_ins":         "Number of rows inserted",
		"n_tup_upd":         "Number of rows updated",
		"n_tup_del":         "Number of rows deleted",
		"n_tup_hot_upd":     "Number of rows HOT updated (i.e., with no separate index update required)",
		"n_live_tup":        "Estimated number of live rows",
		"n_dead_tup":        "Estimated number of dead rows",
	}
)

type TableMetrics struct {
	mutex   sync.Mutex
	name    string
	metrics map[string]prometheus.Gauge
}

func NewTableMetrics(tableName string) *TableMetrics {
	return &TableMetrics{
		name: tableName,
		metrics: map[string]prometheus.Gauge{
			"table_cache_hit_ratio": prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: tableName,
				Name:      "table_cache_hit_ratio",
				Help:      "Table " + tableName + " cache hit ratio",
			}),
			"table_items_count": prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: tableName,
				Name:      "table_items_count",
				Help:      "Table " + tableName + " items count",
			}),
		},
	}
}

func (t *TableMetrics) Scrape(db *sql.DB) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	ratio := new(float64)
	query := "SELECT round(heap_blks_hit*100/(heap_blks_hit+heap_blks_read), 2) AS cache_hit_ratio FROM pg_statio_user_tables" +
		" WHERE relname = $1 AND heap_blks_read > 0 UNION ALL SELECT 0.00 AS cache_hit_ratio ORDER BY cache_hit_ratio DESC LIMIT 1"
	err := db.QueryRow(query, t.name).Scan(ratio)
	if err != nil {
		return errors.New("error running table cache hit stats query on database: " + err.Error())
	}
	t.metrics["table_cache_hit_ratio"].Set(*ratio)

	count := new(float64)
	err = db.QueryRow("SELECT count(*) FROM " + t.name).Scan(count)
	if err != nil {
		return errors.New("error running table items count query on database: " + err.Error())
	}
	t.metrics["table_items_count"].Set(*count)

	err = getMetrics(db, t.metrics, tableMetrics, t.name, "pg_stat_user_tables WHERE relname = $1", []interface{}{t.name})
	if err != nil {
		return errors.New("error running table stats query on database: " + err.Error())
	}

	return nil
}

func (t *TableMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range t.metrics {
		m.Describe(ch)
	}
}

func (t *TableMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, m := range t.metrics {
		m.Collect(ch)
	}
}
