package main

import (
	"database/sql"
	"flag"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"

	"github.com/mc2soft/postgresql_exporter/metrics"
)

const (
	namespace = "postgresql"
)

var (
	listenAddress = flag.String("web.listen-address", ":9104", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	database      = flag.String("db.name", "", "Name of monitored DB.")
	slow          = flag.Int("db.consider-query-slow", 5, "Queries with execution time higher than this value will be considered as slow (in seconds)")
	tables        = flag.String("db.tables", "", "Comma-separated list of tables to track")
)

type Exporter struct {
	m                sync.Mutex
	dsn              string
	metrics          []metrics.Metric
	totalScrapes     prometheus.Counter
	duration, errors prometheus.Gauge
}

func NewPostgreSQLExporter(dsn string) *Exporter {
	e := &Exporter{
		dsn: dsn,
		metrics: []metrics.Metric{
			metrics.NewBufferMetrics(),
			metrics.NewDBMetrics(*database),
			metrics.NewSlowQueryMetrics(*slow),
		},
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total postgresql scrapes.",
		}),
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_duration_seconds",
			Help:      "The last scrape duration.",
		}),

		errors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "exporter_last_scrape_error",
			Help:      "The last scrape error status.",
		}),
	}

	for _, table := range strings.Split(*tables, ",") {
		e.metrics = append(e.metrics, metrics.NewTableMetrics(table))
	}

	return e
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range e.metrics {
		m.Describe(ch)
	}

	ch <- e.duration.Desc()
	ch <- e.totalScrapes.Desc()
	ch <- e.errors.Desc()
}

type metric struct {
	section string
	key     string
	val     string
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	finish := make(chan struct{})

	go e.scrape(finish)
	e.m.Lock()
	defer e.m.Unlock()
	<-finish
	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.errors

	for _, m := range e.metrics {
		m.Collect(ch)
	}
}

func (e *Exporter) scrape(finish chan<- struct{}) {
	defer func() { finish <- struct{}{} }()

	now := time.Now().UnixNano()

	e.totalScrapes.Inc()

	db, err := sql.Open("postgres", e.dsn)
	if err != nil {
		log.Println("error opening connection to database: ", err)
		e.errors.Set(1)
		e.duration.Set(float64(time.Now().UnixNano()-now) / 1000000000)
		return
	}
	defer db.Close()

	for _, m := range e.metrics {
		err = m.Scrape(db)
		if err != nil {
			log.Println(err)
			e.errors.Set(1)
			e.duration.Set(float64(time.Now().UnixNano()-now) / 1000000000)
			return
		}
	}
}

// check interfaces
var _ prometheus.Collector = new(Exporter)

func main() {
	flag.Parse()

	dsn := os.Getenv("DATA_SOURCE_NAME")
	if len(dsn) == 0 {
		log.Fatal("couldn't find environment variable DATA_SOURCE_NAME")
	}

	if *database == "" {
		log.Fatal("please specify database name")
	}

	exporter := NewPostgreSQLExporter(dsn)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
<head><title>PostgreSQL exporter</title></head>
<body>
<h1>>PostgreSQL exporter</h1>
<p><a href='` + *metricPath + `'>Metrics</a></p>
</body>
</html>
`))
	})

	log.Infof("Starting Server: %s", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
