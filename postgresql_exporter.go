package main

import (
	"database/sql"
	"flag"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	namespace     = "postgresql"
	bufferSection = "buffer"
)

var (
	listenAddress = flag.String("web.listen-address", ":9104", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	database      = flag.String("db.name", "", "Name of monitored DB.")
)

var (
	bufferMetrics = map[string]string{
		"buffers_checkpoint":    "Number of buffers written during checkpoints",
		"buffers_clean":         "Number of buffers written by the background writer",
		"maxwritten_clean":      "Number of times the background writer stopped a cleaning scan because it had written too many buffers",
		"buffers_backend":       "Number of buffers written directly by a backend",
		"buffers_backend_fsync": "Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)",
		"buffers_alloc":         "Number of buffers allocated",
	}
)

type Exporter struct {
	m                sync.Mutex
	dsn              string
	metrics          map[string]prometheus.Gauge
	totalScrapes     prometheus.Counter
	duration, errors prometheus.Gauge
}

func NewPostgreSQLExporter(dsn string) *Exporter {
	return &Exporter{
		dsn:     dsn,
		metrics: map[string]prometheus.Gauge{},
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
	scrapes := make(chan metric)

	go e.scrape(scrapes)
	e.m.Lock()
	defer e.m.Unlock()

	for m := range scrapes {
		name := strings.ToLower(m.key)
		value, err := strconv.ParseFloat(m.val, 64)
		if err != nil {
			continue
		}

		if _, ok := e.metrics[name]; !ok {
			e.metrics[name] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: m.section,
				Name:      name,
				Help:      bufferMetrics[name],
			})
		}

		e.metrics[name].Set(value)
	}

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.errors

	for _, m := range e.metrics {
		m.Collect(ch)
	}

}

func (e *Exporter) scrape(scrapes chan<- metric) {
	defer close(scrapes)

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

	var keys []string
	for key := range bufferMetrics {
		keys = append(keys, key)
	}

	// collect buffer metrics

	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = new(string)
	}
	err = db.QueryRow("SELECT " + strings.Join(keys, ",") + " FROM pg_stat_bgwriter").Scan(vals...)
	if err != nil {
		log.Println("error running buffers stats query on database: ", err)
		e.errors.Set(1)
		e.duration.Set(float64(time.Now().UnixNano()-now) / 1000000000)
		return
	}

	for i, val := range vals {
		res := metric{
			section: bufferSection,
			key:     keys[i],
			val:     *val.(*string),
		}

		scrapes <- res
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
