// Package filter checks if the weather data is valid to go to the next stage
package filter

import "worker-weather/internal/dtos"

const rainfallThreshold = 30.0

// IsValid returns true if the following conditions are met:
// + Rainfall is greater than 30mm
func IsValid(weatherData dtos.WeatherData) bool {
	return weatherData.Prec >= rainfallThreshold
}
