package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	batchSize    = 20
	flushTimeout = 2 * time.Second
)

type SensorReading struct {
	SensorID    string    `json:"sensor_id"`
	FieldID     string    `json:"field_id"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature_c"`
	Humidity    float64   `json:"humidity_pct"`
	SoilMoist   float64   `json:"soil_moisture_pct"`
	SoilTemp    float64   `json:"soil_temp_c"`
	Nitrogen    float64   `json:"nitrogen_mg_kg"`
	Phosphorus  float64   `json:"phosphorus_mg_kg"`
	Potassium   float64   `json:"potassium_mg_kg"`
	pH          float64   `json:"ph"`
}

type WeatherReading struct {
	StationID   string    `json:"station_id"`
	Region      string    `json:"region"`
	Timestamp   time.Time `json:"timestamp"`
	Temperature float64   `json:"temperature_c"`
	Humidity    float64   `json:"humidity_pct"`
	Pressure    float64   `json:"pressure_hpa"`
	WindSpeed   float64   `json:"wind_speed_ms"`
	WindDir     float64   `json:"wind_direction_deg"`
	Rainfall    float64   `json:"rainfall_mm"`
	SolarRad    float64   `json:"solar_radiation_wm2"`
	DewPoint    float64   `json:"dew_point_c"`
}

type BatchWriter struct {
	path    string
	ch      chan any
	done    chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
	flushes int
	total   int
}

func newBatchWriter(path string, bufSize int) *BatchWriter {
	bw := &BatchWriter{
		path: path,
		ch:   make(chan any, bufSize),
		done: make(chan struct{}),
	}
	bw.wg.Add(1)
	go bw.loop()
	return bw
}

func (bw *BatchWriter) Send(record any) {
	bw.ch <- record
}

func (bw *BatchWriter) Close() {
	close(bw.ch)
	bw.wg.Wait()
}

func (bw *BatchWriter) Stats() (flushes, total int) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushes, bw.total
}

func (bw *BatchWriter) loop() {
	defer bw.wg.Done()

	f, err := os.Create(bw.path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "batch writer: cannot open %s: %v\n", bw.path, err)
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	batch := make([]any, 0, batchSize)
	ticker := time.NewTicker(flushTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, r := range batch {
			enc.Encode(r)
		}
		bw.mu.Lock()
		bw.flushes++
		bw.total += len(batch)
		bw.mu.Unlock()
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-bw.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

var fields = []string{"field-01", "field-02", "field-03", "field-04", "field-05"}
var regions = []string{"north", "south", "east", "west", "central"}

func collectSensorData(fieldID string, readings int, rng *rand.Rand) []SensorReading {
	result := make([]SensorReading, 0, readings)
	baseTime := time.Now().Add(-time.Duration(readings) * time.Hour)
	baseTemp := 15.0 + rng.Float64()*10
	baseMoist := 30.0 + rng.Float64()*30

	for i := 0; i < readings; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		hourFactor := math.Sin(float64(ts.Hour())*math.Pi/12.0) * 5.0

		result = append(result, SensorReading{
			SensorID:    fmt.Sprintf("sens-%s-%02d", fieldID, rng.Intn(3)+1),
			FieldID:     fieldID,
			Timestamp:   ts,
			Temperature: round(baseTemp+hourFactor+rng.NormFloat64()*0.5, 2),
			Humidity:    round(clamp(60+rng.NormFloat64()*10, 20, 100), 2),
			SoilMoist:   round(clamp(baseMoist+rng.NormFloat64()*2, 0, 100), 2),
			SoilTemp:    round(baseTemp+rng.NormFloat64()*0.3-2, 2),
			Nitrogen:    round(clamp(120+rng.NormFloat64()*20, 0, 300), 2),
			Phosphorus:  round(clamp(45+rng.NormFloat64()*8, 0, 150), 2),
			Potassium:   round(clamp(180+rng.NormFloat64()*25, 0, 400), 2),
			pH:          round(clamp(6.5+rng.NormFloat64()*0.3, 4.0, 9.0), 2),
		})
	}
	return result
}

func collectWeatherData(stationID, region string, readings int, rng *rand.Rand) []WeatherReading {
	result := make([]WeatherReading, 0, readings)
	baseTime := time.Now().Add(-time.Duration(readings) * time.Hour)
	baseTemp := 12.0 + rng.Float64()*12

	for i := 0; i < readings; i++ {
		ts := baseTime.Add(time.Duration(i) * time.Hour)
		hourFactor := math.Sin(float64(ts.Hour())*math.Pi/12.0) * 6.0
		temp := round(baseTemp+hourFactor+rng.NormFloat64()*0.8, 2)
		humidity := round(clamp(65+rng.NormFloat64()*12, 20, 100), 2)
		dewPoint := round(temp-((100-humidity)/5), 2)

		result = append(result, WeatherReading{
			StationID:   stationID,
			Region:      region,
			Timestamp:   ts,
			Temperature: temp,
			Humidity:    humidity,
			Pressure:    round(1013+rng.NormFloat64()*8, 2),
			WindSpeed:   round(clamp(rng.ExpFloat64()*3, 0, 30), 2),
			WindDir:     round(rng.Float64()*360, 1),
			Rainfall:    round(clamp(rng.ExpFloat64()*0.3, 0, 50), 2),
			SolarRad:    round(clamp(500*math.Max(0, math.Sin(float64(ts.Hour())*math.Pi/12))+rng.NormFloat64()*30, 0, 1000), 2),
			DewPoint:    dewPoint,
		})
	}
	return result
}

func round(v, decimals float64) float64 {
	p := math.Pow(10, decimals)
	return math.Round(v*p) / p
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func main() {
	const readingsPerSource = 168

	os.MkdirAll("../data", 0755)

	var wg sync.WaitGroup
	start := time.Now()

	for _, fieldID := range fields {
		wg.Add(1)
		go func(fid string) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			path := fmt.Sprintf("../data/sensors_%s.json", fid)
			bw := newBatchWriter(path, batchSize*2)

			readings := collectSensorData(fid, readingsPerSource, rng)
			for _, r := range readings {
				bw.Send(r)
			}
			bw.Close()

			flushes, total := bw.Stats()
			fmt.Printf("[sensor]  %s: %d records, %d flushes → %s\n", fid, total, flushes, path)
		}(fieldID)
	}

	for i, region := range regions {
		wg.Add(1)
		go func(idx int, reg string) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(idx)))
			stationID := fmt.Sprintf("ws-%s-%02d", reg, idx+1)
			path := fmt.Sprintf("../data/weather_%s.json", reg)
			bw := newBatchWriter(path, batchSize*2)

			readings := collectWeatherData(stationID, reg, readingsPerSource, rng)
			for _, r := range readings {
				bw.Send(r)
			}
			bw.Close()

			flushes, total := bw.Stats()
			fmt.Printf("[weather] %s: %d records, %d flushes → %s\n", reg, total, flushes, path)
		}(i, region)
	}

	wg.Wait()
	fmt.Printf("\nDone in %s. batch_size=%d flush_timeout=%s\n",
		time.Since(start).Round(time.Millisecond), batchSize, flushTimeout)
}
