package bench

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// Result holds a single benchmark measurement.
type Result struct {
	Name       string        `json:"name"`
	N          int           `json:"iterations"`
	NsPerOp    time.Duration `json:"ns_per_op"`
	BytesPerOp int64         `json:"bytes_per_op"`
}

// WriteJSON writes results to w in JSON format.
func WriteJSON(results []Result, w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}

// WriteCSV writes results in CSV format.
func WriteCSV(results []Result, w io.Writer) error {
	c := csv.NewWriter(w)
	if err := c.Write([]string{"name", "n", "ns_per_op", "bytes_per_op"}); err != nil {
		return err
	}
	for _, r := range results {
		record := []string{
			r.Name,
			fmt.Sprintf("%d", r.N),
			fmt.Sprintf("%d", r.NsPerOp.Nanoseconds()),
			fmt.Sprintf("%d", r.BytesPerOp),
		}
		if err := c.Write(record); err != nil {
			return err
		}
	}
	c.Flush()
	return c.Error()
}

// WriteMarkdown renders results as a simple Markdown table.
func WriteMarkdown(results []Result, w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "Benchmark\tns/op\tbytes/op\n")
	fmt.Fprintf(tw, "---------\t-----\t--------\n")
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%d\t%d\n", r.Name, r.NsPerOp.Nanoseconds(), r.BytesPerOp)
	}
	return tw.Flush()
}
