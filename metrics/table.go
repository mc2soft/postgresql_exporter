package metrics

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	tableMetrics = map[string]metric{
		"seq_scan":          metric{Name: "seq_scan_total", Help: "Number of sequential scans initiated on this table"},
		"seq_tup_read":      metric{Name: "seq_tup_read_total", Help: "Number of live rows fetched by sequential scans"},
		"vacuum_count":      metric{Name: "vacuum_count_total", Help: "Number of times this table has been manually vacuumed (not counting VACUUM FULL)"},
		"autovacuum_count":  metric{Name: "autovacuum_count_total", Help: "Number of times this table has been vacuumed by the autovacuum daemon"},
		"analyze_count":     metric{Name: "analyze_count_total", Help: "Number of times this table has been manually analyzed"},
		"autoanalyze_count": metric{Name: "autoanalyze_count_total", Help: "Number of times this table has been analyzed by the autovacuum daemon"},
		"n_tup_ins":         metric{Name: "n_tup_ins_total", Help: "Number of rows inserted"},
		"n_tup_upd":         metric{Name: "n_tup_upd_total", Help: "Number of rows updated"},
		"n_tup_del":         metric{Name: "n_tup_del_total", Help: "Number of rows deleted"},
		"n_tup_hot_upd":     metric{Name: "n_tup_hot_upd_total", Help: "Number of rows HOT updated (i.e., with no separate index update required)"},
		"n_live_tup":        metric{Name: "n_live_tup_total", Help: "Estimated number of live rows"},
		"n_dead_tup":        metric{Name: "n_dead_tup_total", Help: "Estimated number of dead rows"},
	}
)

type TableMetrics struct {
	mutex   sync.Mutex
	names   []string
	metrics map[string]*prometheus.GaugeVec
}

func NewTableMetrics(tableNames []string) *TableMetrics {
	return &TableMetrics{
		names: tableNames,
		metrics: map[string]*prometheus.GaugeVec{
			"table_cache_hit_ratio": prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "tables",
				Name:      "cache_hit_ratio_percent",
				Help:      "Table cache hit ratio",
			}, []string{"table"}),
			"table_items_count": prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "tables",
				Name:      "items_count_total",
				Help:      "Table items count",
			}, []string{"table"}),
			"table_size": prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "tables",
				Name:      "size_bytes",
				Help:      "Total table size including indexes",
			}, []string{"table"}),
		},
	}
}

func (t *TableMetrics) Scrape(db *sql.DB) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if len(t.names) == 1 && t.names[0] == "*" {
		// we will get all tables only once on first scrape
		// so don't forget to restart exporter after adding/removing tables
		names, err := t.getAllTablesForDB(db)
		if err != nil {
			return nil
		}
		t.names = names
	}

	for _, name := range t.names {

		ratio := new(float64)
		query := "SELECT round(heap_blks_hit*100/(heap_blks_hit+heap_blks_read), 2) AS cache_hit_ratio FROM pg_statio_user_tables" +
			" WHERE relname = $1 AND heap_blks_read > 0 UNION ALL SELECT 0.00 AS cache_hit_ratio ORDER BY cache_hit_ratio DESC LIMIT 1"
		err := db.QueryRow(query, name).Scan(ratio)
		if err != nil {
			return errors.New("error running table cache hit stats query on database: " + err.Error())
		}
		t.metrics["table_cache_hit_ratio"].WithLabelValues(name).Set(*ratio)

		count := new(float64)
		err = db.QueryRow("SELECT count(*) FROM " + name).Scan(count)
		if err != nil {
			return errors.New("error running table items count query on database: " + err.Error())
		}
		t.metrics["table_items_count"].WithLabelValues(name).Set(*count)

		size := new(float64)
		err = db.QueryRow("SELECT pg_total_relation_size($1)", name).Scan(size)
		t.metrics["table_size"].WithLabelValues(name).Set(*size)

		result, err := getMetrics(db, tableMetrics, "pg_stat_user_tables WHERE relname = $1", []interface{}{name})
		if err != nil {
			return errors.New("error running table stats query on database: " + err.Error())
		}

		for key, val := range result {
			if _, ok := t.metrics[key]; !ok {
				t.metrics[key] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Namespace: namespace,
					Subsystem: "tables",
					Name:      tableMetrics[key].Name,
					Help:      tableMetrics[key].Help,
				}, []string{"table"})
			}

			t.metrics[key].WithLabelValues(name).Set(val)
		}
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

func (t *TableMetrics) getAllTablesForDB(db *sql.DB) ([]string, error) {
	// get all tables from database and cache them
	// it will happen only first scrape
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return names, nil
}

// check interface
var _ Collection = new(TableMetrics)
