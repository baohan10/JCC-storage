package mq

import "fmt"

const (
	COORDINATOR_QUEUE_NAME = "Coordinator"
	SCANNER_QUEUE_NAME     = "Scanner"
)

func MakeAgentQueueName(id int64) string {
	return fmt.Sprintf("Agent@%d", id)
}
