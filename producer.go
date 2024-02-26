package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/joho/godotenv"
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

	if err := godotenv.Load(); err != nil {
       fmt.Printf("Error loading .env file: %v", err)
    }

	urlSchemaRegistry := os.Getenv("URL_SCHEMA_REGISTRY")
	username := os.Getenv("USERNAME_SCHEMA_REGISTRY")
	password := os.Getenv("PASSWORD_SCHEMA_REGISTRY")

	if urlSchemaRegistry == "" || username == "" || password == "" {
		fmt.Println("Failed to load environment variables to create config Schema Registry")
		os.Exit(1)
	}

	configSchema := schemaregistry.NewConfigWithAuthentication(urlSchemaRegistry, username, password)

	client, err := schemaregistry.NewClient(configSchema)
	if err != nil {
		fmt.Printf("Failed to create schema registry client: %s\n", err)
		os.Exit(1)
	}

	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, jsonschema.NewSerializerConfig())

	if err != nil {
		fmt.Printf("Failed to create serializer: %s\n", err)
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

	order := Order{
		OrderAddress: "Rua Jacinto",
		OrderId: 12,
		OrderTime: 12345,
	}

	order2 := Order{
		OrderAddress: "Rua Goiás Topen",
		OrderId: 12,
		OrderTime: 12345,
	}

	order3 := Order{
		OrderAddress: "Rua Miguel Jovita",
		OrderId: 13,
		OrderTime: 12345,
	}

	order4 := Order{
		OrderAddress: "Rua CT Garra",
		OrderId: 14,
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

		fmt.Println(data, string(dataByte))
		payload, err := ser.Serialize(topic, &data)
		if err != nil {
			fmt.Printf("Failed to serialize payload: %s\n", err)
			os.Exit(1)
		}

		if err != nil {
			fmt.Printf("Failed to convert struct to byte: %s", err)
			os.Exit(1)
		}

		p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            // Key:            []byte(key),
            // Value:          dataByte,
            Value:          payload,
        }, nil)
    }

    // Wait for all messages to be delivered
    p.Flush(15 * 1000)
    p.Close()
}