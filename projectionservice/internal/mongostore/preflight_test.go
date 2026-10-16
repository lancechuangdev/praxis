package mongostore

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"praxis/projectionservice/internal/workflow"
)

func rawDocument(t *testing.T, value any) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPreflightTopology(t *testing.T) {
	valid := bson.M{"setName": "rs0", "isWritablePrimary": true, "logicalSessionTimeoutMinutes": int32(30)}
	if err := validateTopology(rawDocument(t, valid)); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []bson.M{
		{}, {"msg": "isdbgrid", "isWritablePrimary": true, "logicalSessionTimeoutMinutes": 30},
		{"setName": "rs0", "isWritablePrimary": false, "logicalSessionTimeoutMinutes": 30},
		{"setName": "rs0", "isWritablePrimary": true},
		{"setName": "rs0", "isWritablePrimary": true, "logicalSessionTimeoutMinutes": nil},
		{"setName": "rs0", "isWritablePrimary": true, "logicalSessionTimeoutMinutes": 0.5},
	} {
		if err := validateTopology(rawDocument(t, doc)); err == nil {
			t.Fatal("accepted unsupported topology")
		}
	}
}

func TestPreflightCollectionOptions(t *testing.T) {
	for _, options := range []bson.M{{}, {"capped": false}, {"collation": bson.M{"locale": "simple"}}} {
		if err := validateCollection(collectionMetadata{Type: "collection", Options: rawDocument(t, options)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, options := range []bson.M{
		{"capped": true}, {"capped": "false"}, {"collation": bson.M{"locale": "en"}},
		{"collation": nil}, {"timeseries": bson.M{"timeField": "occurred_at"}},
		{"expireAfterSeconds": 0}, {"clusteredIndex": bson.M{}}, {"validator": bson.M{}},
	} {
		if err := validateCollection(collectionMetadata{Type: "collection", Options: rawDocument(t, options)}); err == nil {
			t.Fatalf("accepted unsafe options %v", options)
		}
	}
	for _, kind := range []string{"view", "timeseries", ""} {
		if err := validateCollection(collectionMetadata{Type: kind}); err == nil {
			t.Fatal("accepted noncollection")
		}
	}
}

func TestPreflightIndexes(t *testing.T) {
	id := rawDocument(t, bson.M{"name": "_id_", "key": bson.M{"_id": 1}})
	if err := validateIndexes([]bson.Raw{id}); err != nil {
		t.Fatal(err)
	}
	secondary := rawDocument(t, bson.M{"name": "by_user", "key": bson.M{"user_id": 1}})
	if err := validateIndexes([]bson.Raw{id, secondary}); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []bson.M{
		{"name": "expires", "key": bson.M{"occurred_at": 1}, "expireAfterSeconds": 0},
		{"name": "expires", "key": bson.M{"occurred_at": 1}, "expireAfterSeconds": nil},
		{"name": "owners", "key": bson.M{"user_id": 1}, "unique": true},
		{"name": "partial", "key": bson.M{"user_id": 1}, "partialFilterExpression": bson.M{}},
		{"name": "localized", "key": bson.M{"user_id": 1}, "collation": bson.M{"locale": "en"}},
	} {
		if err := validateIndexes([]bson.Raw{id, rawDocument(t, unsafe)}); err == nil {
			t.Fatal("accepted unsafe index")
		}
	}
	for _, invalid := range []bson.M{
		{"name": "_id_", "key": bson.M{"wrong": 1}},
		{"name": "_id_", "key": bson.M{"_id": 1.5}},
		{"name": "_id_", "key": bson.M{"_id": 1}, "unique": false},
	} {
		if err := validateIndexes([]bson.Raw{rawDocument(t, invalid)}); err == nil {
			t.Fatal("accepted invalid identity index")
		}
	}
	if err := validateIndexes([]bson.Raw{secondary}); err == nil {
		t.Fatal("accepted missing identity index")
	}
}

func TestUnverifiedStoreCannotProcess(t *testing.T) {
	s := &Store{target: workflow.Target{Generation: "orders", Mode: workflow.Shadow}}
	if err := s.Within(context.Background(), s.target, func(workflow.Transaction) error { t.Fatal("unverified callback ran"); return nil }); !errors.Is(err, ErrUnverified) {
		t.Fatal(err)
	}
}
