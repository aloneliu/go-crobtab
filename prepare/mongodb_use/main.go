package main

import (
	"github.com/mongodb/mongo-go-driver/mongo"
	"context"
	"time"
	"log"
	"fmt"
	"github.com/mongodb/mongo-go-driver/options"
)

func main() {
	// 建立连接
	client, err := mongo.Connect(context.TODO(), "mongodb://127.0.0.1:27017", options.Client().SetConnectTimeout(5*time.Second))
	if err != nil {
		log.Println(err)
	}

	// 选择数据库 选择表
	collection := client.Database("cron").Collection("log")

	data := logRecord{
		Name:      "hello",
		CreatedAt: time.Now(),
	}

	result, err := collection.InsertOne(context.TODO(), data)
	if err != nil {
		log.Println(err)
	}

	fmt.Println(result.InsertedID)

}

type logRecord struct {
	Name      string    `bson:name`
	CreatedAt time.Time `bson:createdAt`
}
