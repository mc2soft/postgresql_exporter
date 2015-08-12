package metrics

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "postgresql"
)

type metric struct {
	Name string
	Help string
}

type Collection interface {
	Scrape(*sql.DB) error
	Collect(chan<- prometheus.Metric)
	Describe(chan<- *prometheus.Desc)
}

func getMetrics(db *sql.DB, metricsDef map[string]metric, tail string, args []interface{}) (map[string]float64, error) {
	var keys []string
	for key := range metricsDef {
		keys = append(keys, key)
	}

	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = new(float64)
	}
	err := db.QueryRow("SELECT "+strings.Join(keys, ",")+" FROM "+tail, args...).Scan(vals...)
	if err != nil {
		return nil, errors.New("error running buffers stats query on database: " + err.Error())
	}

	result := make(map[string]float64)
	for i, val := range vals {
		key := keys[i]
		result[key] = *val.(*float64)
	}

	return result, nil
}
