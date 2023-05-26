package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/client/internal/config"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
)

type AgentService struct {
	*Service
}

func AgentSvc(svc *Service) *AgentService {
	return &AgentService{Service: svc}
}

func (svc *AgentService) PostEvent(nodeID int, event any, isEmergency bool, dontMerge bool) error {
	agentClient, err := agtcli.NewAgentClient(nodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
	}
	defer agentClient.Close()

	err = agentClient.PostEvent(event, isEmergency, dontMerge)
	if err != nil {
		return fmt.Errorf("request to agent %d failed, err: %w", nodeID, err)
	}

	return nil
}
