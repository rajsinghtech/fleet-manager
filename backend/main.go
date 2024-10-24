package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
	"github.com/teslamotors/fleet-telemetry/protos"
)

// Config holds the entire configuration structure
type Config struct {
	Kafka KafkaConfig
	AWS   *S3Config
	Local *LocalConfig
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	Enabled   bool
	Protocol  string
	Host      string
	Port      string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
}

// LocalConfig holds local backup configuration
type LocalConfig struct {
	Enabled  bool
	BasePath string
}

// KafkaConfig holds Kafka consumer configuration
type KafkaConfig struct {
	BootstrapServers string
	GroupID          string
	AutoOffsetReset  string
	Topic            string
}

// Service encapsulates the application's dependencies
type Service struct {
	Config             Config
	S3Client           *s3.S3
	LocalBackupEnabled bool
	LocalBasePath      string
	KafkaConsumer      *kafka.Consumer
	PrometheusMetrics  *Metrics
}

// Metrics holds all Prometheus metrics
type Metrics struct {
	Gauge     *prometheus.GaugeVec
	Latitude  *prometheus.GaugeVec
	Longitude *prometheus.GaugeVec
}

// NewService initializes the service with configurations
func NewService(cfg Config) (*Service, error) {
	service := &Service{
		Config:             cfg,
		LocalBackupEnabled: cfg.Local != nil && cfg.Local.Enabled,
		LocalBasePath:      "",
		PrometheusMetrics:  initializePrometheusMetrics(),
	}

	if service.LocalBackupEnabled {
		service.LocalBasePath = cfg.Local.BasePath
	}

	// Initialize AWS S3 if enabled
	if cfg.AWS != nil && cfg.AWS.Enabled {
		s3Client, err := configureS3(cfg.AWS)
		if err != nil {
			return nil, fmt.Errorf("failed to configure S3: %w", err)
		}
		service.S3Client = s3Client

		if err := testS3Connection(s3Client, cfg.AWS.Bucket); err != nil {
			return nil, fmt.Errorf("S3 connection test failed: %w", err)
		}
		log.Println("S3 connection established successfully.")
	} else {
		log.Println("AWS S3 uploads are disabled.")
	}

	// Initialize Kafka consumer
	consumer, err := configureKafka(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Kafka consumer: %w", err)
	}
	service.KafkaConsumer = consumer

	return service, nil
}

// initializePrometheusMetrics sets up Prometheus metrics
func initializePrometheusMetrics() *Metrics {
	metrics := &Metrics{
		Gauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vehicle_data",
				Help: "Vehicle data metrics",
			},
			[]string{"field", "vin"},
		),
		Latitude: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vehicle_data_latitude",
				Help: "Vehicle latitude metrics",
			},
			[]string{"field", "vin"},
		),
		Longitude: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vehicle_data_longitude",
				Help: "Vehicle longitude metrics",
			},
			[]string{"field", "vin"},
		),
	}

	prometheus.MustRegister(metrics.Gauge, metrics.Latitude, metrics.Longitude)
	return metrics
}

// configureS3 sets up the AWS S3 client
func configureS3(s3Config *S3Config) (*s3.S3, error) {
	if err := validateS3Config(s3Config); err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s://%s:%s", s3Config.Protocol, s3Config.Host, s3Config.Port)

	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(s3Config.Region),
		Credentials:      credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
		Endpoint:         aws.String(endpoint),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS session: %w", err)
	}

	return s3.New(sess), nil
}

// validateS3Config ensures all required S3 configurations are present
func validateS3Config(cfg *S3Config) error {
	if cfg.Protocol == "" || cfg.Host == "" || cfg.Port == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" {
		return fmt.Errorf("incomplete S3 configuration")
	}
	return nil
}

// testS3Connection verifies the connection to S3 by listing objects in the specified bucket
func testS3Connection(s3Svc *s3.S3, bucket string) error {
	_, err := s3Svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return fmt.Errorf("failed to access S3 bucket '%s': %w", bucket, err)
	}
	return nil
}

// configureKafka sets up the Kafka consumer
func configureKafka(kafkaCfg KafkaConfig) (*kafka.Consumer, error) {
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  kafkaCfg.BootstrapServers,
		"group.id":           kafkaCfg.GroupID,
		"auto.offset.reset":  kafkaCfg.AutoOffsetReset,
		"enable.auto.commit": false, // Manual commit for better control
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kafka consumer: %w", err)
	}

	// Subscribe to the specified topic
	if err := consumer.SubscribeTopics([]string{kafkaCfg.Topic}, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to Kafka topic '%s': %w", kafkaCfg.Topic, err)
	}

	return consumer, nil
}

// loadConfigFromEnv reads and validates the configuration from environment variables
func loadConfigFromEnv() (Config, error) {
	var cfg Config

	// Kafka configuration
	cfg.Kafka.BootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	cfg.Kafka.GroupID = os.Getenv("KAFKA_GROUP_ID")
	cfg.Kafka.AutoOffsetReset = os.Getenv("KAFKA_AUTO_OFFSET_RESET")
	if cfg.Kafka.AutoOffsetReset == "" {
		cfg.Kafka.AutoOffsetReset = "earliest"
	}
	cfg.Kafka.Topic = os.Getenv("KAFKA_TOPIC")

	if cfg.Kafka.BootstrapServers == "" || cfg.Kafka.GroupID == "" || cfg.Kafka.Topic == "" {
		return cfg, fmt.Errorf("incomplete Kafka configuration")
	}

	// AWS S3 configuration
	awsEnabledStr := os.Getenv("AWS_ENABLED")
	awsEnabled, err := strconv.ParseBool(awsEnabledStr)
	if err != nil {
		awsEnabled = false
	}
	if awsEnabled {
		cfg.AWS = &S3Config{
			Enabled:   true,
			Protocol:  os.Getenv("AWS_BUCKET_PROTOCOL"),
			Host:      os.Getenv("AWS_BUCKET_HOST"),
			Port:      os.Getenv("AWS_BUCKET_PORT"),
			Bucket:    os.Getenv("AWS_BUCKET_NAME"),
			AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			Region:    os.Getenv("AWS_BUCKET_REGION"),
		}
		if err := validateS3Config(cfg.AWS); err != nil {
			return cfg, fmt.Errorf("invalid AWS configuration: %w", err)
		}
	}

	// Local backup configuration
	localEnabledStr := os.Getenv("LOCAL_ENABLED")
	localEnabled, err := strconv.ParseBool(localEnabledStr)
	if err != nil {
		localEnabled = false
	}
	if localEnabled {
		cfg.Local = &LocalConfig{
			Enabled:  true,
			BasePath: os.Getenv("LOCAL_BASE_PATH"),
		}
		if cfg.Local.BasePath == "" {
			return cfg, fmt.Errorf("LOCAL_BASE_PATH cannot be empty when local backup is enabled")
		}
	}

	return cfg, nil
}

// uploadToS3 uploads JSON data to the specified S3 bucket with a vin/year/month/day key structure
func uploadToS3(s3Svc *s3.S3, bucket, vin string, data []byte) error {
	now := time.Now().UTC()
	key := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d%02d%02d.%06dZ.json",
		vin,
		now.Year(),
		now.Month(),
		now.Day(),
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000,
	)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}

	_, err := s3Svc.PutObject(input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 at key '%s': %w", key, err)
	}

	log.Printf("Successfully uploaded data to S3 at key: %s", key)
	return nil
}

// backupLocally saves JSON data to the local filesystem with a vin/year/month/day folder structure
func backupLocally(basePath, vin string, data []byte) error {
	now := time.Now().UTC()
	dirPath := filepath.Join(basePath,
		vin,
		fmt.Sprintf("%04d", now.Year()),
		fmt.Sprintf("%02d", now.Month()),
		fmt.Sprintf("%02d", now.Day()),
	)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories '%s': %w", dirPath, err)
	}

	fileName := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%06dZ.json",
		now.Year(), now.Month(), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000)

	filePath := filepath.Join(dirPath, fileName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file '%s': %w", filePath, err)
	}

	log.Printf("Successfully backed up data locally at: %s", filePath)
	return nil
}

// processValue handles different types of Protobuf values and updates Prometheus metrics
func processValue(datum *protos.Datum, service *Service, vin string) {
	fieldName := datum.Key.String()

	switch v := datum.Value.Value.(type) {
	case *protos.Value_StringValue:
		handleStringValue(v.StringValue, fieldName, service, vin)
	case *protos.Value_IntValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.IntValue))
	case *protos.Value_LongValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.LongValue))
	case *protos.Value_FloatValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.FloatValue))
	case *protos.Value_DoubleValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(v.DoubleValue)
	case *protos.Value_BooleanValue:
		numericValue := boolToFloat64(v.BooleanValue)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(numericValue)
	case *protos.Value_LocationValue:
		// Update separate Latitude and Longitude metrics with the field name as a label
		service.PrometheusMetrics.Latitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Latitude)
		service.PrometheusMetrics.Longitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Longitude)
	case *protos.Value_DoorValue:
		handleDoorValues(v.DoorValue, service.PrometheusMetrics.Gauge, vin)
	case *protos.Value_TimeValue:
		totalSeconds := float64(v.TimeValue.Hour*3600 + v.TimeValue.Minute*60 + v.TimeValue.Second)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(totalSeconds)
	// Handle enums by setting their integer values
	case *protos.Value_ChargingValue:
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(float64(v.ChargingValue))
	// Add other enum cases as needed
	case *protos.Value_Invalid:
		log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	default:
		log.Printf("Unhandled value type for field '%s': %T", fieldName, v)
	}
}

// boolToFloat64 converts a boolean to float64 (1.0 for true, 0.0 for false)
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}

// handleStringValue processes string values, attempting to parse them as floats
func handleStringValue(stringValue, fieldName string, service *Service, vin string) {
	if stringValue == "<invalid>" || stringValue == "\u003cinvalid\u003e" {
		log.Printf("Invalid string value received for field '%s', setting as NaN", fieldName)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
		return
	}

	floatVal, err := strconv.ParseFloat(stringValue, 64)
	if err == nil {
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(floatVal)
	} else {
		log.Printf("Non-numeric string value received for field '%s': '%s', setting as NaN", fieldName, stringValue)
		service.PrometheusMetrics.Gauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	}
}

// handleDoorValues processes door states from Protobuf and updates Prometheus metrics
func handleDoorValues(doors *protos.Doors, gauge *prometheus.GaugeVec, vin string) {
	doorFields := map[string]bool{
		"DriverFront":    doors.DriverFront,
		"PassengerFront": doors.PassengerFront,
		"DriverRear":     doors.DriverRear,
		"PassengerRear":  doors.PassengerRear,
		"TrunkFront":     doors.TrunkFront,
		"TrunkRear":      doors.TrunkRear,
	}

	for doorName, state := range doorFields {
		numericValue := boolToFloat64(state)
		gauge.WithLabelValues(doorName, vin).Set(numericValue)
	}
}

// startPrometheusServer launches the Prometheus metrics HTTP server
func startPrometheusServer(addr string, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Run server in a separate goroutine
	go func() {
		log.Printf("Starting Prometheus metrics server at %s/metrics", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Prometheus HTTP server failed: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Shutdown the server gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Prometheus HTTP server shutdown failed: %v", err)
	} else {
		log.Println("Prometheus HTTP server shut down gracefully.")
	}
}

// startConsumerLoop begins consuming Kafka messages
func startConsumerLoop(service *Service, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("Starting Kafka message consumption...")
	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumption loop exiting due to context cancellation.")
			return
		default:
			msg, err := service.KafkaConsumer.ReadMessage(-1)
			if err != nil {
				// Handle Kafka consumer errors
				if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrAllBrokersDown {
					log.Printf("Kafka broker is down: %v", err)
					time.Sleep(5 * time.Second) // Wait before retrying
					continue
				}
				log.Printf("Error while consuming message: %v", err)
				continue
			}

			// Deserialize the Protobuf message
			vehicleData := &protos.Payload{}
			if err := proto.Unmarshal(msg.Value, vehicleData); err != nil {
				log.Printf("Failed to unmarshal Protobuf message: %v", err)
				continue
			}

			log.Printf("Received Vehicle Data for VIN %s", vehicleData.Vin)

			// Process each Datum in the Payload
			for _, datum := range vehicleData.Data {
				processValue(datum, service, vehicleData.Vin)
			}

			// Serialize data as JSON for storage
			serializedData, err := json.Marshal(vehicleData)
			if err != nil {
				log.Printf("Failed to marshal vehicleData to JSON: %v", err)
				continue
			}

			// Upload to S3 if enabled
			if service.S3Client != nil {
				if err := uploadToS3(service.S3Client, service.Config.AWS.Bucket, vehicleData.Vin, serializedData); err != nil {
					log.Printf("Failed to upload vehicle data to S3: %v", err)
				}
			}

			// Backup locally if enabled
			if service.LocalBackupEnabled {
				if err := backupLocally(service.LocalBasePath, vehicleData.Vin, serializedData); err != nil {
					log.Printf("Failed to backup vehicle data locally: %v", err)
				}
			}

			// Commit the message offset after successful processing
			if _, err := service.KafkaConsumer.CommitMessage(msg); err != nil {
				log.Printf("Failed to commit Kafka message: %v", err)
			}
		}
	}
}

// main is the entry point of the application
func main() {
	// Load configuration from environment variables
	cfg, err := loadConfigFromEnv()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Read Prometheus address from environment variable, default to ":2112"
	promAddr := os.Getenv("PROMETHEUS_ADDR")
	if promAddr == "" {
		promAddr = ":2112"
	}

	// Initialize service
	service, err := NewService(cfg)
	if err != nil {
		log.Fatalf("Service initialization error: %v", err)
	}

	// Setup context and wait group for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start Prometheus metrics server
	wg.Add(1)
	go startPrometheusServer(promAddr, &wg, ctx)

	// Start Kafka consumer loop
	wg.Add(1)
	go startConsumerLoop(service, ctx, &wg)

	// Setup signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigchan
	log.Printf("Received signal: %v. Initiating shutdown...", sig)

	// Initiate shutdown
	cancel()

	// Close Kafka consumer
	if err := service.KafkaConsumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v", err)
	} else {
		log.Println("Kafka consumer closed successfully.")
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("Application shut down gracefully.")
}