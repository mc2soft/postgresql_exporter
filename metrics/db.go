package metrics

import (
	"database/sql"
	"errors"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	dbMetrics = map[string]string{
		"numbackends":   "Number of backends currently connected to this database",
		"tup_returned":  "Number of rows returned by queries in this database",
		"tup_fetched":   "Number of rows fetched by queries in this database",
		"tup_inserted":  "Number of rows inserted by queries in this database",
		"tup_updated":   "Number of rows updated by queries in this database",
		"tup_deleted":   "Number of rows deleted by queries in this database",
		"xact_commit":   "Number of transactions in this database that have been committed",
		"xact_rollback": "Number of transactions in this database that have been rolled back",
		"deadlocks":     "Number of deadlocks detected in this database",
		"temp_files":    "Number of temporary files created by queries in this database",
		"temp_bytes":    "Total amount of data written to temporary files by queries in this database",
	}
)

type DBMetrics struct {
	mutex   sync.Mutex
	name    string
	metrics map[string]prometheus.Gauge
}

func NewDBMetrics(dbName string) *DBMetrics {
	d := &DBMetrics{
		name:    dbName,
		metrics: map[string]prometheus.Gauge{},
	}
	d.metrics["db_size"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "postgresql",
		Subsystem: "database_" + d.name,
		Name:      "db_size",
		Help:      "Size of database",
	})
	d.metrics["cache_hit_ratio"] = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "postgresql",
		Subsystem: "database_" + d.name,
		Name:      "cache_hit_ratio",
		Help:      "Cache hit ratio",
	})

	return d
}

func (d *DBMetrics) Scrape(db *sql.DB) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	size := new(float64)
	err := db.QueryRow("SELECT pg_database_size($1) FROM pg_database WHERE datname = $1", d.name).Scan(size)
	if err != nil {
		return errors.New("failed to get database size: " + err.Error())
	}
	d.metrics["db_size"].Set(*size)

	var keys []string
	for key := range dbMetrics {
		keys = append(keys, key)
	}

	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = new(float64)
	}
	err = db.QueryRow("SELECT "+strings.Join(keys, ",")+" FROM pg_stat_database WHERE datname = $1", d.name).Scan(vals...)
	if err != nil {
		return errors.New("error running database stats query on database: " + err.Error())
	}

	for i, val := range vals {
		key := keys[i]
		if _, ok := d.metrics[key]; !ok {
			d.metrics[key] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: "postgresql",
				Subsystem: "database_" + d.name,
				Name:      key,
				Help:      dbMetrics[key],
			})
		}

		d.metrics[key].Set(*val.(*float64))
	}

	cacheRatio := new(float64)
	query := "SELECT round(blks_hit*100/(blks_hit+blks_read), 2) AS cache_hit_ratio FROM pg_stat_database WHERE datname = $1 and blks_read > 0 union all select 0.00 AS cache_hit_ratio ORDER BY cache_hit_ratio DESC limit 1"
	err = db.QueryRow(query, d.name).Scan(cacheRatio)
	if err != nil {
		return errors.New("failed to get database cache hit ratio: " + err.Error())
	}
	d.metrics["cache_hit_ratio"].Set(*cacheRatio)

	return nil
}

func (d *DBMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range d.metrics {
		m.Describe(ch)
	}
}

func (d *DBMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, m := range d.metrics {
		m.Collect(ch)
	}
}
