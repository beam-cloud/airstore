package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// OpenWeatherMap API endpoint
const openWeatherMapAPI = "https://api.openweathermap.org/data/2.5"

// Weather command constants - must match YAML definition
const (
	weatherCmdCurrent  = "current"
	weatherCmdForecast = "forecast"
	weatherCmdHourly   = "hourly"
)

// WeatherClient implements the Weather tool using OpenWeatherMap API
type WeatherClient struct {
	httpClient *http.Client
	apiKey     string
}

// NewWeatherClient creates a new Weather client
func NewWeatherClient(apiKey string) *WeatherClient {
	return &WeatherClient{
		httpClient: &http.Client{},
		apiKey:     apiKey,
	}
}

// Name returns the tool identifier - implements ToolClient interface
func (w *WeatherClient) Name() types.ToolName {
	return types.ToolWeather
}

// Execute runs a Weather command - implements ToolClient interface
func (w *WeatherClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error {
	// Weather uses API key from constructor, creds can override
	_ = creds // Reserved for future credential override
	var result any
	var err error

	switch command {
	case weatherCmdCurrent:
		location, _ := args["location"].(string)
		units := GetStringArg(args, "units", "metric")
		result, err = w.getCurrent(ctx, location, units)

	case weatherCmdForecast:
		location, _ := args["location"].(string)
		units := GetStringArg(args, "units", "metric")
		days := GetIntArg(args, "days", 5)
		result, err = w.getForecast(ctx, location, units, days)

	case weatherCmdHourly:
		location, _ := args["location"].(string)
		units := GetStringArg(args, "units", "metric")
		hours := GetIntArg(args, "hours", 24)
		result, err = w.getHourly(ctx, location, units, hours)

	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	if err != nil {
		return err
	}

	// Output as JSON
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// CurrentWeatherResponse represents current weather data
type CurrentWeatherResponse struct {
	Location    string  `json:"location"`
	Country     string  `json:"country"`
	Temperature float64 `json:"temperature"`
	FeelsLike   float64 `json:"feels_like"`
	Humidity    int     `json:"humidity"`
	Pressure    int     `json:"pressure"`
	WindSpeed   float64 `json:"wind_speed"`
	WindDeg     int     `json:"wind_deg"`
	Clouds      int     `json:"clouds"`
	Visibility  int     `json:"visibility"`
	Weather     string  `json:"weather"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	Sunrise     int64   `json:"sunrise"`
	Sunset      int64   `json:"sunset"`
	Timezone    int     `json:"timezone"`
	Units       string  `json:"units"`
}

func (w *WeatherClient) getCurrent(ctx context.Context, location, units string) (*CurrentWeatherResponse, error) {
	params := url.Values{
		"appid": {w.apiKey},
		"units": {units},
	}

	// Check if location is coordinates (contains comma and looks like numbers)
	if isCoordinates(location) {
		parts := strings.Split(location, ",")
		params.Set("lat", strings.TrimSpace(parts[0]))
		params.Set("lon", strings.TrimSpace(parts[1]))
	} else {
		params.Set("q", location)
	}

	reqURL := openWeatherMapAPI + "/weather?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("location not found: %s", location)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("invalid API key")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("weather API error: %s - %s", resp.Status, string(body))
	}

	var apiResp struct {
		Name string `json:"name"`
		Sys  struct {
			Country string `json:"country"`
			Sunrise int64  `json:"sunrise"`
			Sunset  int64  `json:"sunset"`
		} `json:"sys"`
		Main struct {
			Temp      float64 `json:"temp"`
			FeelsLike float64 `json:"feels_like"`
			Humidity  int     `json:"humidity"`
			Pressure  int     `json:"pressure"`
		} `json:"main"`
		Wind struct {
			Speed float64 `json:"speed"`
			Deg   int     `json:"deg"`
		} `json:"wind"`
		Clouds struct {
			All int `json:"all"`
		} `json:"clouds"`
		Visibility int `json:"visibility"`
		Weather    []struct {
			Main        string `json:"main"`
			Description string `json:"description"`
			Icon        string `json:"icon"`
		} `json:"weather"`
		Timezone int `json:"timezone"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	result := &CurrentWeatherResponse{
		Location:    apiResp.Name,
		Country:     apiResp.Sys.Country,
		Temperature: apiResp.Main.Temp,
		FeelsLike:   apiResp.Main.FeelsLike,
		Humidity:    apiResp.Main.Humidity,
		Pressure:    apiResp.Main.Pressure,
		WindSpeed:   apiResp.Wind.Speed,
		WindDeg:     apiResp.Wind.Deg,
		Clouds:      apiResp.Clouds.All,
		Visibility:  apiResp.Visibility,
		Sunrise:     apiResp.Sys.Sunrise,
		Sunset:      apiResp.Sys.Sunset,
		Timezone:    apiResp.Timezone,
		Units:       units,
	}

	if len(apiResp.Weather) > 0 {
		result.Weather = apiResp.Weather[0].Main
		result.Description = apiResp.Weather[0].Description
		result.Icon = apiResp.Weather[0].Icon
	}

	return result, nil
}

// ForecastDay represents a single day's forecast
type ForecastDay struct {
	Date        string  `json:"date"`
	TempMin     float64 `json:"temp_min"`
	TempMax     float64 `json:"temp_max"`
	Humidity    int     `json:"humidity"`
	Weather     string  `json:"weather"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	WindSpeed   float64 `json:"wind_speed"`
	Pop         float64 `json:"precipitation_probability"`
}

// ForecastResponse represents the forecast data
type ForecastResponse struct {
	Location string        `json:"location"`
	Country  string        `json:"country"`
	Days     []ForecastDay `json:"days"`
	Units    string        `json:"units"`
}

func (w *WeatherClient) getForecast(ctx context.Context, location, units string, days int) (*ForecastResponse, error) {
	if days < 1 {
		days = 1
	}
	if days > 5 {
		days = 5
	}

	params := url.Values{
		"appid": {w.apiKey},
		"units": {units},
		"cnt":   {strconv.Itoa(days * 8)}, // 8 data points per day (3-hour intervals)
	}

	if isCoordinates(location) {
		parts := strings.Split(location, ",")
		params.Set("lat", strings.TrimSpace(parts[0]))
		params.Set("lon", strings.TrimSpace(parts[1]))
	} else {
		params.Set("q", location)
	}

	reqURL := openWeatherMapAPI + "/forecast?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("location not found: %s", location)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("weather API error: %s - %s", resp.Status, string(body))
	}

	var apiResp struct {
		City struct {
			Name    string `json:"name"`
			Country string `json:"country"`
		} `json:"city"`
		List []struct {
			DtTxt string `json:"dt_txt"`
			Main  struct {
				TempMin  float64 `json:"temp_min"`
				TempMax  float64 `json:"temp_max"`
				Humidity int     `json:"humidity"`
			} `json:"main"`
			Weather []struct {
				Main        string `json:"main"`
				Description string `json:"description"`
				Icon        string `json:"icon"`
			} `json:"weather"`
			Wind struct {
				Speed float64 `json:"speed"`
			} `json:"wind"`
			Pop float64 `json:"pop"`
		} `json:"list"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Aggregate by day
	dayMap := make(map[string]*ForecastDay)
	for _, item := range apiResp.List {
		date := strings.Split(item.DtTxt, " ")[0]
		if _, ok := dayMap[date]; !ok {
			dayMap[date] = &ForecastDay{
				Date:    date,
				TempMin: item.Main.TempMin,
				TempMax: item.Main.TempMax,
			}
		}

		day := dayMap[date]
		if item.Main.TempMin < day.TempMin {
			day.TempMin = item.Main.TempMin
		}
		if item.Main.TempMax > day.TempMax {
			day.TempMax = item.Main.TempMax
		}
		day.Humidity = item.Main.Humidity
		day.WindSpeed = item.Wind.Speed
		day.Pop = item.Pop

		if len(item.Weather) > 0 {
			day.Weather = item.Weather[0].Main
			day.Description = item.Weather[0].Description
			day.Icon = item.Weather[0].Icon
		}
	}

	// Convert to sorted slice
	forecastDays := make([]ForecastDay, 0, len(dayMap))
	for _, day := range dayMap {
		forecastDays = append(forecastDays, *day)
	}

	// Limit to requested days
	if len(forecastDays) > days {
		forecastDays = forecastDays[:days]
	}

	return &ForecastResponse{
		Location: apiResp.City.Name,
		Country:  apiResp.City.Country,
		Days:     forecastDays,
		Units:    units,
	}, nil
}

// HourlyForecastItem represents a single hour's forecast
type HourlyForecastItem struct {
	Time        string  `json:"time"`
	Temperature float64 `json:"temperature"`
	FeelsLike   float64 `json:"feels_like"`
	Humidity    int     `json:"humidity"`
	Weather     string  `json:"weather"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	WindSpeed   float64 `json:"wind_speed"`
	Pop         float64 `json:"precipitation_probability"`
}

// HourlyForecastResponse represents hourly forecast data
type HourlyForecastResponse struct {
	Location string               `json:"location"`
	Country  string               `json:"country"`
	Hours    []HourlyForecastItem `json:"hours"`
	Units    string               `json:"units"`
}

func (w *WeatherClient) getHourly(ctx context.Context, location, units string, hours int) (*HourlyForecastResponse, error) {
	if hours < 1 {
		hours = 1
	}
	if hours > 48 {
		hours = 48
	}

	// Use forecast endpoint (gives 3-hour intervals, best we can do with free API)
	params := url.Values{
		"appid": {w.apiKey},
		"units": {units},
		"cnt":   {strconv.Itoa((hours + 2) / 3)}, // Convert hours to 3-hour intervals
	}

	if isCoordinates(location) {
		parts := strings.Split(location, ",")
		params.Set("lat", strings.TrimSpace(parts[0]))
		params.Set("lon", strings.TrimSpace(parts[1]))
	} else {
		params.Set("q", location)
	}

	reqURL := openWeatherMapAPI + "/forecast?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("location not found: %s", location)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("weather API error: %s - %s", resp.Status, string(body))
	}

	var apiResp struct {
		City struct {
			Name    string `json:"name"`
			Country string `json:"country"`
		} `json:"city"`
		List []struct {
			DtTxt string `json:"dt_txt"`
			Main  struct {
				Temp      float64 `json:"temp"`
				FeelsLike float64 `json:"feels_like"`
				Humidity  int     `json:"humidity"`
			} `json:"main"`
			Weather []struct {
				Main        string `json:"main"`
				Description string `json:"description"`
				Icon        string `json:"icon"`
			} `json:"weather"`
			Wind struct {
				Speed float64 `json:"speed"`
			} `json:"wind"`
			Pop float64 `json:"pop"`
		} `json:"list"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	hourlyItems := make([]HourlyForecastItem, 0, len(apiResp.List))
	for _, item := range apiResp.List {
		hourItem := HourlyForecastItem{
			Time:        item.DtTxt,
			Temperature: item.Main.Temp,
			FeelsLike:   item.Main.FeelsLike,
			Humidity:    item.Main.Humidity,
			WindSpeed:   item.Wind.Speed,
			Pop:         item.Pop,
		}

		if len(item.Weather) > 0 {
			hourItem.Weather = item.Weather[0].Main
			hourItem.Description = item.Weather[0].Description
			hourItem.Icon = item.Weather[0].Icon
		}

		hourlyItems = append(hourlyItems, hourItem)
	}

	return &HourlyForecastResponse{
		Location: apiResp.City.Name,
		Country:  apiResp.City.Country,
		Hours:    hourlyItems,
		Units:    units,
	}, nil
}

// Helper functions

func isCoordinates(s string) bool {
	if !strings.Contains(s, ",") {
		return false
	}
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return false
	}
	_, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	_, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	return err1 == nil && err2 == nil
}
