package entity

import "github.com/pkg/errors"

var (
	errServiceUnhealthy          = errors.New("error service unhealthy")
	errKillingService            = errors.New("error killing service")
	errBringingBackToLifeService = errors.New("error bringing back to life")
	errSettingConnection         = errors.New("error setting connection")
	errDockerClient              = errors.New("error docker client initialization")
)
