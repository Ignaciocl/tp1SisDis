package tp1SisDis

type Station struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Year int    `json:"year"`
}

type WorkerStation struct {
	City string  `json:"city"`
	Data Station `json:"data,omitempty"`
	Key  string  `json:"key"`
	EOF  bool    `json:"EOF"`
}

type JoinerDataStation struct {
	Code string `json:"code"`
	Name string `json:"name"`
	Key  string `json:"key"`
	EOF  bool   `json:"EOF"`
}
