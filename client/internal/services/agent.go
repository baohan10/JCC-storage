package services

type AgentService struct {
	*Service
}

func (svc *Service) AgentSvc() *AgentService {
	return &AgentService{Service: svc}
}
