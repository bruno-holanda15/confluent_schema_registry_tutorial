package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Order struct {
	OrderAddress string `json:"orderAddress"`
	OrderId int `json:"orderId"`
	OrderTime int `json:"orderTime"`
}

func main() {

    if len(os.Args) != 2 {
        fmt.Fprintf(os.Stderr, "Usage: %s <config-file-path>\n",
		os.Args[0])
        os.Exit(1)
    }
    configFile := os.Args[1]
    conf := ReadConfig(configFile)

    topic := "orders"
    p, err := kafka.NewProducer(&conf)

    if err != nil {
        fmt.Printf("Failed to create producer: %s", err)
        os.Exit(1)
    }

    // Go-routine to handle message delivery reports and
    // possibly other event types (errors, stats, etc)
    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
                        *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
                }
            }
        }
    }()

    // users := [...]string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	order := Order{
		OrderAddress: "Rua Flores",
		OrderId: 1,
		OrderTime: 12345,
	}

	order2 := Order{
		OrderAddress: "Rua Goiás",
		OrderId: 2,
		OrderTime: 12345,
	}

	order3 := Order{
		OrderAddress: "Rua Jovita",
		OrderId: 3,
		OrderTime: 12345,
	}

	order4 := Order{
		OrderAddress: "Rua Garra",
		OrderId: 3,
		OrderTime: 12345,
	}

    items := [...]Order{order, order2, order3, order4}

    for n := 0; n < 3; n++ {
        // key := users[rand.Intn(len(users))]
        data := items[rand.Intn(len(items))]
		
		dataByte, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Failed to convert struct to byte: %s", err)
			os.Exit(1)
		}

        p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            // Key:            []byte(key),
            Value:          dataByte,
        }, nil)
    }

    // Wait for all messages to be delivered
    p.Flush(15 * 1000)
    p.Close()
}