package handler

import (
	"github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) HandleMessage(message []byte, topic kafka.TopicPartition, cn int) error {
	logrus.Infof("Consumer #%d, Message from kafka with offset %d '%s' on partition %d", cn, topic.Offset, string(message), topic.Partition)
	return nil
}
