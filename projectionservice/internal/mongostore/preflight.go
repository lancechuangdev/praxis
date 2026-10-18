package mongostore

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var ErrUnverified = errors.New("workflow storage requires successful preflight")

// Verify is read-only. It permits an existing generation to be opened without
// createCollection privileges. Any failure revokes this instance's readiness;
// this is a point-in-time configuration check, not protection from later DDL.
func (s *Store) Verify(ctx context.Context) error {
	return s.verifyCollections(ctx, s.snapshots, s.inbox)
}

func (s *Store) verifyCollections(ctx context.Context, collections ...*mongo.Collection) error {
	s.verified.Store(false)
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var hello bson.Raw
	if err := s.db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return fmt.Errorf("workflow topology preflight: %w", err)
	}
	if err := validateTopology(hello); err != nil {
		return err
	}
	for _, collection := range collections {
		cursor, err := s.db.ListCollections(ctx, bson.D{{Key: "name", Value: collection.Name()}})
		if err != nil {
			return fmt.Errorf("workflow collection preflight: %w", err)
		}
		var metadata []collectionMetadata
		err = cursor.All(ctx, &metadata)
		if err != nil {
			return err
		}
		if len(metadata) != 1 || metadata[0].Name != collection.Name() {
			return errors.New("required workflow collection is missing")
		}
		if err := validateCollection(metadata[0]); err != nil {
			return err
		}
		indexes, err := collection.Indexes().List(ctx)
		if err != nil {
			return err
		}
		var specs []bson.Raw
		if err := indexes.All(ctx, &specs); err != nil {
			return err
		}
		if err := validateIndexes(specs); err != nil {
			return err
		}
	}
	s.verified.Store(true)
	return nil
}

type collectionMetadata struct {
	Name    string   `bson:"name"`
	Type    string   `bson:"type"`
	Options bson.Raw `bson:"options"`
}

func validateTopology(hello bson.Raw) error {
	set, _ := hello.Lookup("setName").StringValueOK()
	primary, _ := hello.Lookup("isWritablePrimary").BooleanOK()
	timeout := hello.Lookup("logicalSessionTimeoutMinutes")
	minutes, ok := integerValue(timeout)
	if set == "" || !primary || !ok || minutes <= 0 {
		return errors.New("workflow storage requires a writable replica-set primary with logical sessions; standalone and mongos endpoints are unsupported")
	}
	return nil
}

func validateCollection(metadata collectionMetadata) error {
	if metadata.Type != "collection" {
		return errors.New("workflow storage requires ordinary collections")
	}
	if len(metadata.Options) == 0 {
		return errors.New("workflow collection options are missing")
	}
	elements, err := metadata.Options.Elements()
	if err != nil {
		return err
	}
	for _, element := range elements {
		switch element.Key() {
		case "capped":
			value, ok := element.Value().BooleanOK()
			if !ok || value {
				return errors.New("capped workflow collections are unsupported")
			}
		case "collation":
			collation, ok := element.Value().DocumentOK()
			if !ok {
				return errors.New("invalid workflow collation")
			}
			locale, ok := collation.Lookup("locale").StringValueOK()
			if !ok || locale != "simple" {
				return errors.New("workflow collections require binary collation")
			}
		default:
			return fmt.Errorf("unsupported workflow collection option %q", element.Key())
		}
	}
	return nil
}

func validateIndexes(indexes []bson.Raw) error {
	foundID := false
	for _, index := range indexes {
		// Reject by presence, including zero, null and malformed TTL options.
		if _, err := index.LookupErr("expireAfterSeconds"); err == nil {
			return errors.New("workflow collections must not have TTL indexes")
		}
		if _, err := index.LookupErr("partialFilterExpression"); err == nil {
			return errors.New("partial workflow indexes are unsupported")
		}
		if raw, err := index.LookupErr("collation"); err == nil {
			collation, ok := raw.DocumentOK()
			if !ok {
				return errors.New("invalid workflow index collation")
			}
			locale, ok := collation.Lookup("locale").StringValueOK()
			if !ok || locale != "simple" {
				return errors.New("workflow indexes require binary collation")
			}
		}
		name, _ := index.Lookup("name").StringValueOK()
		key, ok := index.Lookup("key").DocumentOK()
		if !ok {
			return errors.New("invalid workflow index key")
		}
		if name == "_id_" {
			elements, err := key.Elements()
			if err != nil {
				return err
			}
			value, valid := integerValue(key.Lookup("_id"))
			if len(elements) != 1 || !valid || value != 1 {
				return errors.New("invalid workflow _id index")
			}
			// MongoDB's built-in _id index is unique even when unique is omitted.
			if raw, err := index.LookupErr("unique"); err == nil {
				value, ok := raw.BooleanOK()
				if !ok || !value {
					return errors.New("workflow _id index must be unique")
				}
			}
			foundID = true
		} else if raw, err := index.LookupErr("unique"); err == nil {
			value, ok := raw.BooleanOK()
			if !ok || value {
				return errors.New("additional unique workflow indexes are unsupported")
			}
		}
	}
	if !foundID {
		return errors.New("workflow collection requires its built-in _id index")
	}
	return nil
}

func integerValue(value bson.RawValue) (int64, bool) {
	if value.Type != bson.TypeInt32 && value.Type != bson.TypeInt64 {
		return 0, false
	}
	return value.AsInt64OK()
}
