package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"praxis/orderservice/internal/order"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "projection export failed:", err)
		os.Exit(1)
	}
}
func run() error {
	output := flag.String("output", "", "new JSONL export path")
	flag.Parse()
	if *output == "" || flag.NArg() != 0 {
		return fmt.Errorf("--output is required")
	}
	url := os.Getenv("ORDER_DATABASE_URL")
	if url == "" {
		return fmt.Errorf("ORDER_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("database setup failed")
	}
	defer db.Close()
	file, err := os.CreateTemp(filepath.Dir(*output), ".projection-export-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	writer := &boundedWriter{writer: file, remaining: 128 * 1024 * 1024}
	if err := order.ExportWorkflow(ctx, db, writer); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Link publishes the complete artifact atomically and refuses to overwrite.
	return os.Link(file.Name(), *output)
}

type boundedWriter struct {
	writer    io.Writer
	remaining int
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, fmt.Errorf("export exceeds 128 MiB")
	}
	n, err := w.writer.Write(p)
	w.remaining -= n
	return n, err
}
