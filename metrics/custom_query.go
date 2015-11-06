package metrics

import (
	"database/sql"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

type CustomQueryMetrics struct {
	mutex sync.Mutex
	cq    []CustomQuery
}

func NewCustomQueryMetrics(cq []CustomQuery) *CustomQueryMetrics {
	return &CustomQueryMetrics{cq: cq}
}

func (c *CustomQueryMetrics) Scrape(db *sql.DB) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := range c.cq {
		err := c.cq[i].scrape(db)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CustomQueryMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, query := range c.cq {
		query.describe(ch)
	}
}

func (c *CustomQueryMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, query := range c.cq {
		query.collect(ch)
	}
}

type CustomQuery struct {
	Name  string
	Help  string
	Query string

	colsCount int
	metric    *prometheus.GaugeVec
}

func (c *CustomQuery) initMetric(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	c.colsCount = len(cols)

	var labelNames []string
	if len(cols) > 0 {
		labelNames = cols[1:]
	}
	c.metric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "custom",
		Name:      c.Name,
		Help:      c.Help,
	}, labelNames)

	log.Infof("Initialized custom metric %s %s", c.Name, c.Query)

	return nil
}

func (c *CustomQuery) scrape(db *sql.DB) error {
	rows, err := db.Query(c.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if c.metric == nil {
		err := c.initMetric(rows)
		if err != nil {
			return err
		}
	}

	for rows.Next() {
		var count int64
		labels := make([]string, c.colsCount-1)
		args := make([]interface{}, c.colsCount-1)
		for i := range args {
			args[i] = &labels[i]
		}
		args = append([]interface{}{&count}, args...)

		err = rows.Scan(args...)
		if err != nil {
			return err
		}

		c.metric.WithLabelValues(labels...).Set(float64(count))
	}
	return nil
}

func (c *CustomQuery) describe(ch chan<- *prometheus.Desc) {
	if c.metric != nil {
		c.metric.Describe(ch)
	}
}

func (c *CustomQuery) collect(ch chan<- prometheus.Metric) {
	if c.metric != nil {
		c.metric.Collect(ch)
	}
}

// check interface
var _ Collection = new(CustomQueryMetrics)
