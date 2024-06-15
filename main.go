package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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

func rollback(ctx context.Context, client *kgo.Client) {
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Printf("error aborting buffered records: %v\n", err) // this only happens if ctx is canceled
		return
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}

func dispatchEvent(ctx context.Context, event Event) {
	producerId := strconv.FormatInt(int64(os.Getpid()), 10)
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.TransactionalID(producerId),
		kgo.DefaultProduceTopic("tuyau-broadcast"),
	)

	if err != nil {
		fmt.Printf("error initializing Kafka producer client: %v\n", err)
		return
	}

	defer client.Close()

	if err := client.BeginTransaction(); err != nil {
		fmt.Printf("error beginning transaction: %v\n", err)
	}

	fmt.Println("Transaction began")

	// Write some messages in the transaction.
	data, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("error beginning transaction: %v\n", err)
		rollback(ctx, client)
	}

	fmt.Printf("JSON done: %s\n", string(data))

	if err := client.ProduceSync(ctx, kgo.StringRecord(string(data))).FirstErr(); err != nil {
		fmt.Printf("NOT PRODUCED %+v", err)
		rollback(ctx, client)
	}

	if err := client.Flush(ctx); err != nil {
		fmt.Printf("flush was killed due to context cancelation\n")
	}

	switch err := client.EndTransaction(ctx, kgo.TryCommit); err {
	case nil:
	case kerr.OperationNotAttempted:
		println("NOT ATTEMPTED")
		rollback(ctx, client)
	default:
		fmt.Printf("error committing transaction: %v\n", err)
	}

	fmt.Println("producer exited")
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

		println("Dispatching event")
		dispatchEvent(ctx, currentEvent)
	}
}
