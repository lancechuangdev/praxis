package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"praxis/projectionservice/internal/config"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/mongostore"
	"praxis/projectionservice/internal/runtime"
	"praxis/projectionservice/internal/workflow"
)

func configure(ctx context.Context, cfg config.Config, client *mongo.Client, prepare, live bool) (runtime.Consumer, func(), error) {
	var apply runtime.ApplyRecord
	var init func(context.Context) error
	if cfg.Stream == "journal" {
		store, err := mongostore.NewJournal(client, cfg.Database, journal.Target{Generation: cfg.Target.Generation, Mode: journal.Mode(cfg.Target.Mode)}, cfg.TransactionTimeout)
		if err != nil {
			return nil, nil, err
		}
		init = store.Verify
		if prepare {
			init = store.Prepare
		}
		p, err := journal.New(journal.Target{Generation: cfg.Target.Generation, Mode: journal.Mode(cfg.Target.Mode)}, store)
		if err != nil {
			return nil, nil, err
		}
		apply = func(ctx context.Context, r mongostore.QueueRecord) error {
			_, err := p.Process(ctx, r.Topic, r.Key, r.Payload)
			return err
		}
	} else {
		store, err := mongostore.New(client, cfg.Database, cfg.Target, cfg.TransactionTimeout)
		if err != nil {
			return nil, nil, err
		}
		init = store.Verify
		if prepare {
			init = store.Prepare
		}
		p, err := workflow.New(cfg.Target, store)
		if err != nil {
			return nil, nil, err
		}
		apply = func(ctx context.Context, r mongostore.QueueRecord) error {
			_, err := p.Process(ctx, r.Topic, r.Key, r.Payload)
			return err
		}
	}
	if err := init(ctx); err != nil {
		return nil, nil, fmt.Errorf("projection storage preflight/preparation failed")
	}
	queue, err := mongostore.NewQueue(client, cfg.Database, cfg.Target, cfg.Stream, cfg.TransactionTimeout)
	if err != nil {
		return nil, nil, err
	}
	init = queue.Verify
	if prepare {
		init = queue.Prepare
	}
	if err := init(ctx); err != nil {
		return nil, nil, fmt.Errorf("projection recovery queue preflight/preparation failed")
	}
	if prepare {
		ops, err := mongostore.NewOperations(client, cfg.Database, cfg.Target, cfg.Stream, cfg.TransactionTimeout)
		if err != nil {
			return nil, nil, err
		}
		if err := ops.Prepare(ctx); err != nil {
			return nil, nil, err
		}
		return nil, func() {}, nil
	}
	if !live {
		return &runtime.Queued{Queue: queue, Apply: apply}, func() {}, nil
	}
	source, err := kafkaconsumer.NewQueued(cfg.Kafka, queue)
	if err != nil {
		return nil, nil, err
	}
	return &runtime.Queued{Source: source, Queue: queue, Apply: apply}, func() { _ = source.Close() }, nil
}
