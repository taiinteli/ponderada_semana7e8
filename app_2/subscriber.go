package main

import (
    "fmt"
    "os"
    "github.com/joho/godotenv"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)


func consumeMessages(consumer *kafka.Consumer, topic string) {
	consumer.SubscribeTopics([]string{topic}, nil)
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message: %s\n", string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}
}

func main() {
	err := godotenv.Load(".env")
	// Configurações do consumidor
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("BOOTSTRAP_SERVERS"),
		"group.id":          os.Getenv("CLUSTER_ID"),
		"auto.offset.reset": "earliest",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     os.Getenv("API_KEY"),
		"sasl.password":     os.Getenv("API_SECRET"),
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := "teste"

	// Iniciar consumidor em uma goroutine separada
	go consumeMessages(consumer, topic)

	// Manter o programa em execução
	select {}
}