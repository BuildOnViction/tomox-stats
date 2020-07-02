package crons

import (
	"github.com/robfig/cron"
	"github.com/tomochain/tomox-stats/app"
	"github.com/tomochain/tomox-stats/services"
)

// CronService contains the services required to initialize crons
type CronService struct {
	RelayService *services.RelayerService
}

// NewCronService returns a new instance of CronService
func NewCronService(
	relayService *services.RelayerService,
) *CronService {
	return &CronService{
		RelayService: relayService,
	}
}

// InitCrons is responsible for initializing all the crons in the system
func (s *CronService) InitCrons() {

	c := cron.New()
	if app.Config.RunFullnode {
		s.RelayService.UpdateRelayers()
		s.startRelayerUpdate(c)
	}
	c.Start()
}
