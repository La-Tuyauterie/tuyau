package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type EventDocumentKey struct {
	ID primitive.ObjectID `bson:"_id"`
}

type EventID struct {
	Data string `bson:"_data"`
}

type EventNamespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type Event struct {
	ID            EventID          `bson:"_id"`
	OperationType string           `bson:"operationType"`
	ClusterTime   time.Time        `bson:"clusterTime"`
	WallTime      time.Time        `bson:"wallTime"`
	FullDocument  interface{}      `bson:"fullDocument"`
	Namespace     EventNamespace   `bson:"ns"`
	DocumentKey   EventDocumentKey `bson:"documentKey"`
}

func main() {
	ctx := context.Background()

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}

	db := client.Database("tuyau-test-1")
	cs, err := db.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		panic(err)
	}

	defer cs.Close(ctx)

	for cs.Next(ctx) {

		var currentEvent Event
		err := cs.Decode(&currentEvent)
		if err != nil {
			panic(err)
		}

		// send elsewhere
	}
}
