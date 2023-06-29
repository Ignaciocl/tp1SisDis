package domain

import "github.com/pkg/errors"

var (
	errInitializingSender          = errors.New("error initializing sender")
	errInitializingReceiver        = errors.New("error initializing receiver")
	errInitializingGracefulManager = errors.New("error initializing graceful manager")
	errCreatingPublisher           = errors.New("error creating publisher")
	errReceivingData               = errors.New("error receiving data")
)
