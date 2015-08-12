package metrics

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	dbMetrics = map[string]metric{
		"numbackends":   metric{Name: "numbackends_total", Help: "Number of backends currently connected to this database"},
		"tup_returned":  metric{Name: "tup_returned_total", Help: "Number of rows returned by queries in this database"},
		"tup_fetched":   metric{Name: "tup_fetched_total", Help: "Number of rows fetched by queries in this database"},
		"tup_inserted":  metric{Name: "tup_inserted_total", Help: "Number of rows inserted by queries in this database"},
		"tup_updated":   metric{Name: "tup_updated_total", Help: "Number of rows updated by queries in this database"},
		"tup_deleted":   metric{Name: "tup_deleted_total", Help: "Number of rows deleted by queries in this database"},
		"xact_commit":   metric{Name: "xact_commit_total", Help: "Number of transactions in this database that have been committed"},
		"xact_rollback": metric{Name: "xact_rollback_total", Help: "Number of transactions in this database that have been rolled back"},
		"deadlocks":     metric{Name: "deadlocks_total", Help: "Number of deadlocks detected in this database"},
		"temp_files":    metric{Name: "temp_files_total", Help: "Number of temporary files created by queries in this database"},
		"temp_bytes":    metric{Name: "temp_bytes", Help: "Total amount of data written to temporary files by queries in this database"},
	}
)

type DBMetrics struct {
	mutex   sync.Mutex
	names   []string
	metrics map[string]*prometheus.GaugeVec
}

func NewDBMetrics(dbNames []string) *DBMetrics {
	d := &DBMetrics{
		names: dbNames,
		metrics: map[string]*prometheus.GaugeVec{
			"size": prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "databases",
				Name:      "size_bytes",
				Help:      "Size of database",
			}, []string{"db"}),
			"cache_hit_ratio": prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "databases",
				Name:      "cache_hit_ratio_percents",
				Help:      "Cache hit ratio",
			}, []string{"db"}),
		},
	}

	return d
}

func (d *DBMetrics) Scrape(db *sql.DB) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, name := range d.names {
		size := new(float64)
		err := db.QueryRow("SELECT pg_database_size($1) FROM pg_database WHERE datname = $1", name).Scan(size)
		if err != nil {
			return errors.New("failed to get database size: " + err.Error())
		}
		d.metrics["size"].WithLabelValues(name).Set(*size)

		results, err := getMetrics(db, dbMetrics, "pg_stat_database WHERE datname = $1", []interface{}{name})
		if err != nil {
			return errors.New("error running database stats query on database: " + err.Error())
		}

		for key, val := range results {
			if _, ok := d.metrics[key]; !ok {
				d.metrics[key] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Namespace: namespace,
					Subsystem: "databases",
					Name:      dbMetrics[key].Name,
					Help:      dbMetrics[key].Help,
				}, []string{"db"})
			}

			d.metrics[key].WithLabelValues(name).Set(val)
		}

		cacheRatio := new(float64)
		query := "SELECT round(blks_hit*100/(blks_hit+blks_read), 2) AS cache_hit_ratio FROM pg_stat_database WHERE datname = $1 and blks_read > 0 union all select 0.00 AS cache_hit_ratio ORDER BY cache_hit_ratio DESC limit 1"
		err = db.QueryRow(query, name).Scan(cacheRatio)
		if err != nil {
			return errors.New("failed to get database cache hit ratio: " + err.Error())
		}
		d.metrics["cache_hit_ratio"].WithLabelValues(name).Set(*cacheRatio)
	}

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

// check interface
var _ Collection = new(DBMetrics)
