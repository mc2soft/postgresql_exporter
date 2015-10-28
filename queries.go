package main

import (
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

type customQuery struct {
	Query       string
	LabelsCount int
	Counter     *prometheus.CounterVec
	Gauge       *prometheus.GaugeVec
}

func parseQueries(queriesPath string) (cq []customQuery) {
	if queriesPath == "" {
		return
	}

	f, err := os.Open(queriesPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}

	rows := make(map[string]string)
	err = yaml.Unmarshal(b, rows)
	if err != nil {
		log.Fatal(err)
	}

	rx := regexp.MustCompile(`select (.*) from`)
	for key, query := range rows {
		matches := rx.FindStringSubmatch(strings.ToLower(query))
		if len(matches) != 2 {
			log.Fatal("Couldn't parse columns from select clause", key, query)
		}
		columns := strings.Split(matches[1], ",")

		var labelNames []string
		for _, column := range columns[:len(columns)-1] {
			labelNames = append(labelNames, strings.Trim(column, " "))
		}

		custom := customQuery{Query: query, LabelsCount: len(labelNames)}
		if strings.Contains(query, "gauge_value") {
			// gauge
			custom.Gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      key,
				Help:      query,
			}, labelNames)

		} else {
			//counter
			custom.Counter = prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: namespace,
				Name:      key,
				Help:      query,
			}, labelNames)
		}

		cq = append(cq, custom)
	}

	return
}
