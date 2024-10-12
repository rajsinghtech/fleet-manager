package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/teslamotors/fleet-telemetry/protos"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Config holds the entire configuration structure
type Config struct {
	Kafka      KafkaConfig       `yaml:"kafka"`
	AWS        *S3Config         `yaml:"aws,omitempty"`
	Local      *LocalConfig      `yaml:"local,omitempty"`
	ClickHouse *ClickHouseConfig `yaml:"clickhouse,omitempty"`
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	Enabled   bool   `yaml:"enabled"`
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"accessKey"`
	SecretKey string `yaml:"secretKey"`
	Region    string `yaml:"region"`
}

// LocalConfig holds local backup configuration
type LocalConfig struct {
	Enabled  bool   `yaml:"enabled"`
	BasePath string `yaml:"basePath"`
}

// KafkaConfig holds Kafka consumer configuration
type KafkaConfig struct {
	BootstrapServers string `yaml:"bootstrap.servers"`
	GroupID          string `yaml:"group.id"`
	AutoOffsetReset  string `yaml:"auto.offset.reset"`
	Topic            string `yaml:"topic"`
}

// ClickHouseConfig holds ClickHouse database configuration
type ClickHouseConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Database  string `yaml:"database"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Secure    bool   `yaml:"secure"`
	TableName string `yaml:"table_name,omitempty"`
}

// Service encapsulates the application's dependencies
type Service struct {
	Config              Config
	S3Client            *s3.S3
	LocalBackupEnabled  bool
	LocalBasePath       string
	KafkaConsumer       *kafka.Consumer
	PrometheusGauge     *prometheus.GaugeVec
	PrometheusLatitude  *prometheus.GaugeVec
	PrometheusLongitude *prometheus.GaugeVec
	ClickHouseClient    clickhouse.Conn
	ClickHouseEnabled   bool
	ClickHouseTableName string
}

// NewService initializes the service with configurations
func NewService(cfg Config) (*Service, error) {
	service := &Service{
		Config:             cfg,
		LocalBackupEnabled: cfg.Local != nil && cfg.Local.Enabled,
	}

	if service.LocalBackupEnabled {
		service.LocalBasePath = cfg.Local.BasePath
	}

	// Initialize AWS S3 if configuration is provided and enabled
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
		log.Println("AWS S3 integration is disabled or not configured.")
	}

	// Initialize ClickHouse if configuration is provided and enabled
	if cfg.ClickHouse != nil && cfg.ClickHouse.Enabled {
		chClient, err := configureClickHouse(cfg.ClickHouse)
		if err != nil {
			return nil, fmt.Errorf("failed to configure ClickHouse: %w", err)
		}
		service.ClickHouseClient = chClient
		service.ClickHouseEnabled = true

		if err := testClickHouseConnection(chClient); err != nil {
			return nil, fmt.Errorf("ClickHouse connection test failed: %w", err)
		}
		log.Println("ClickHouse connection established successfully.")

		// Set ClickHouse table name, defaulting if not provided
		if cfg.ClickHouse.TableName != "" {
			service.ClickHouseTableName = cfg.ClickHouse.TableName
		} else {
			service.ClickHouseTableName = "vehicle_data"
		}

		// Load existing data into ClickHouse
		if err := loadExistingDataIntoClickHouse(service); err != nil {
			return nil, fmt.Errorf("failed to load existing data into ClickHouse: %w", err)
		}
	} else {
		log.Println("ClickHouse integration is disabled or not configured.")
	}

	// Initialize Kafka consumer
	consumer, err := configureKafka(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("failed to configure Kafka consumer: %w", err)
	}
	service.KafkaConsumer = consumer

	// Initialize Prometheus metrics
	service.initializePrometheusMetrics()

	return service, nil
}

// initializePrometheusMetrics sets up Prometheus metrics
func (s *Service) initializePrometheusMetrics() {
	s.PrometheusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data",
			Help: "Vehicle data metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(s.PrometheusGauge)

	s.PrometheusLatitude = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data_latitude",
			Help: "Vehicle latitude metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(s.PrometheusLatitude)

	s.PrometheusLongitude = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vehicle_data_longitude",
			Help: "Vehicle longitude metrics",
		},
		[]string{"field", "vin"},
	)
	prometheus.MustRegister(s.PrometheusLongitude)
}

// configureS3 sets up the AWS S3 client
func configureS3(cfg *S3Config) (*s3.S3, error) {
	if err := validateS3Config(cfg); err != nil {
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(cfg.Region),
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
		Endpoint:         aws.String(cfg.Endpoint),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS session: %w", err)
	}

	return s3.New(sess), nil
}

// validateS3Config ensures all required S3 configurations are present
func validateS3Config(cfg *S3Config) error {
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" || cfg.Region == "" {
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

// configureClickHouse sets up the ClickHouse client
func configureClickHouse(cfg *ClickHouseConfig) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: !cfg.Secure,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to connect to ClickHouse: %w", err)
	}
	return conn, nil
}

// testClickHouseConnection verifies the connection to ClickHouse
func testClickHouseConnection(conn clickhouse.Conn) error {
	return conn.Ping(context.Background())
}

// loadExistingDataIntoClickHouse loads existing JSON data from S3 or local storage into ClickHouse
func loadExistingDataIntoClickHouse(service *Service) error {
	log.Println("Loading existing data from storage into ClickHouse...")
	var files []string
	var err error

	if service.S3Client != nil {
		files, err = listS3JSONFiles(service.S3Client, service.Config.AWS.Bucket)
		if err != nil {
			return fmt.Errorf("failed to list JSON files in S3: %w", err)
		}
	} else if service.LocalBackupEnabled {
		files, err = listLocalJSONFiles(service.LocalBasePath)
		if err != nil {
			return fmt.Errorf("failed to list JSON files locally: %w", err)
		}
	} else {
		return fmt.Errorf("no storage (S3 or local) configured to load data from")
	}

	for _, file := range files {
		var data []byte
		if service.S3Client != nil {
			data, err = downloadS3JSONFile(service.S3Client, service.Config.AWS.Bucket, file)
			if err != nil {
				log.Printf("Failed to download S3 file '%s': %v", file, err)
				continue
			}
		} else if service.LocalBackupEnabled {
			data, err = os.ReadFile(file)
			if err != nil {
				log.Printf("Failed to read local file '%s': %v", file, err)
				continue
			}
		}

		vehicleData := &protos.Payload{}
		if err := protojson.Unmarshal(data, vehicleData); err != nil {
			log.Printf("Failed to unmarshal JSON data from file '%s': %v", file, err)
			continue
		}

		if err := insertIntoClickHouse(service.ClickHouseClient, service.ClickHouseTableName, vehicleData); err != nil {
			log.Printf("Failed to insert data from file '%s' into ClickHouse: %v", file, err)
			continue
		}

		log.Printf("Successfully loaded data from '%s' into ClickHouse.", file)
	}

	log.Println("Completed loading existing data into ClickHouse.")
	return nil
}

// listS3JSONFiles lists all JSON files in the specified S3 bucket
func listS3JSONFiles(s3Svc *s3.S3, bucket string) ([]string, error) {
	var files []string
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(""),
	}

	err := s3Svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".json") {
				files = append(files, *obj.Key)
			}
		}
		return !lastPage
	})

	if err != nil {
		return nil, err
	}
	return files, nil
}

// listLocalJSONFiles lists all JSON files in the specified local directory
func listLocalJSONFiles(basePath string) ([]string, error) {
	var files []string
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// downloadS3JSONFile downloads a JSON file from S3
func downloadS3JSONFile(s3Svc *s3.S3, bucket, key string) ([]byte, error) {
	output, err := s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// insertIntoClickHouse inserts vehicle data into ClickHouse
func insertIntoClickHouse(conn clickhouse.Conn, tableName string, data *protos.Payload) error {
	ctx := context.Background()

	// Ensure the target table exists. This is optional and can be handled separately.
	createTableQuery := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		vin String,
		timestamp DateTime,
		data String
	) ENGINE = MergeTree()
	ORDER BY (vin, timestamp)
	`, tableName)
	if err := conn.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create ClickHouse table: %w", err)
	}

	// Prepare the batch insert
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (vin, timestamp, data)", tableName))
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %w", err)
	}

	// Serialize the data to JSON string
	jsonData, err := protojson.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	// Use the current UTC time as the timestamp
	timestamp := time.Now().UTC()

	// Append data to the batch
	if err := batch.Append(data.Vin, timestamp, string(jsonData)); err != nil {
		return fmt.Errorf("failed to append data to ClickHouse batch: %w", err)
	}

	// Send the batch to ClickHouse
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %w", err)
	}

	return nil
}

// configureKafka sets up the Kafka consumer
func configureKafka(cfg KafkaConfig) (*kafka.Consumer, error) {
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"auto.offset.reset":  cfg.AutoOffsetReset,
		"enable.auto.commit": false,
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kafka consumer: %w", err)
	}

	// Subscribe to the specified topic
	if err := consumer.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to Kafka topic '%s': %w", cfg.Topic, err)
	}

	return consumer, nil
}

// loadConfig reads and unmarshals the YAML configuration file
func loadConfig(path string) (Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("error reading config file '%s': %w", path, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("error unmarshalling config file: %w", err)
	}

	// Validate Kafka configuration
	if cfg.Kafka.BootstrapServers == "" || cfg.Kafka.GroupID == "" || cfg.Kafka.AutoOffsetReset == "" || cfg.Kafka.Topic == "" {
		return cfg, fmt.Errorf("incomplete Kafka configuration")
	}

	// Validate ClickHouse configuration if provided and enabled
	if cfg.ClickHouse != nil && cfg.ClickHouse.Enabled {
		if cfg.ClickHouse.Host == "" || cfg.ClickHouse.Port == 0 || cfg.ClickHouse.Database == "" ||
			cfg.ClickHouse.Username == "" || cfg.ClickHouse.Password == "" {
			return cfg, fmt.Errorf("incomplete ClickHouse configuration")
		}
	}

	// Validate AWS configuration if provided and enabled
	if cfg.AWS != nil && cfg.AWS.Enabled {
		if err := validateS3Config(cfg.AWS); err != nil {
			return cfg, fmt.Errorf("invalid AWS configuration: %w", err)
		}
	}

	// Validate Local backup configuration if provided and enabled
	if cfg.Local != nil && cfg.Local.Enabled {
		if cfg.Local.BasePath == "" {
			return cfg, fmt.Errorf("Local backup is enabled but basePath is not specified")
		}
	}

	return cfg, nil
}

// uploadToS3 uploads Protobuf data as JSON to the specified S3 bucket
func uploadToS3(s3Svc *s3.S3, bucket, vin string, data *protos.Payload) error {
	now := time.Now().UTC()
	key := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d%02d%02d.%06dZ.json",
		vin,
		now.Year(),
		int(now.Month()),
		now.Day(),
		now.Year(), int(now.Month()), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000,
	)

	jsonData, err := protojson.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(jsonData),
		ContentType: aws.String("application/json"),
	}

	_, err = s3Svc.PutObject(input)
	if err != nil {
		return fmt.Errorf("failed to upload to S3 at key '%s': %w", key, err)
	}

	log.Printf("Successfully uploaded data to S3 at key: %s", key)
	return nil
}

// backupLocally saves Protobuf data as JSON to the local filesystem
func backupLocally(basePath, vin string, data *protos.Payload) error {
	now := time.Now().UTC()
	dirPath := filepath.Join(basePath,
		vin,
		fmt.Sprintf("%04d", now.Year()),
		fmt.Sprintf("%02d", int(now.Month())),
		fmt.Sprintf("%02d", now.Day()),
	)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories '%s': %w", dirPath, err)
	}

	fileName := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%06dZ.json",
		now.Year(), int(now.Month()), now.Day(),
		now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000)

	filePath := filepath.Join(dirPath, fileName)

	// Serialize to JSON
	jsonData, err := protojson.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
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
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.IntValue))
	case *protos.Value_LongValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.LongValue))
	case *protos.Value_FloatValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(v.FloatValue))
	case *protos.Value_DoubleValue:
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(v.DoubleValue)
	case *protos.Value_BooleanValue:
		numericValue := boolToFloat64(v.BooleanValue)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(numericValue)
	case *protos.Value_LocationValue:
		service.PrometheusLatitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Latitude)
		service.PrometheusLongitude.WithLabelValues(fieldName, vin).Set(v.LocationValue.Longitude)
	case *protos.Value_DoorValue:
		handleDoorValues(v.DoorValue, service.PrometheusGauge, vin)
	case *protos.Value_TimeValue:
		totalSeconds := float64(v.TimeValue.Hour*3600 + v.TimeValue.Minute*60 + v.TimeValue.Second)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(totalSeconds)
	case *protos.Value_Invalid:
		log.Printf("Invalid value received for field '%s', setting as NaN", fieldName)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	default:
		handleEnumValues(v, fieldName, service, vin)
	}
}

// handleEnumValues processes enum values
func handleEnumValues(value interface{}, fieldName string, service *Service, vin string) {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Int32 && val.Kind() != reflect.Int64 {
		log.Printf("Unhandled value type for field '%s': %v", fieldName, value)
		return
	}
	service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(float64(val.Int()))
}

// boolToFloat64 converts a boolean to float64
func boolToFloat64(value bool) float64 {
	if value {
		return 1.0
	}
	return 0.0
}

// handleStringValue processes string values
func handleStringValue(stringValue, fieldName string, service *Service, vin string) {
	if stringValue == "<invalid>" || stringValue == "\u003cinvalid\u003e" {
		log.Printf("Invalid string value received for field '%s', setting as NaN", fieldName)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
		return
	}

	floatVal, err := strconv.ParseFloat(stringValue, 64)
	if err == nil {
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(floatVal)
	} else {
		log.Printf("Non-numeric string value received for field '%s': '%s', setting as NaN", fieldName, stringValue)
		service.PrometheusGauge.WithLabelValues(fieldName, vin).Set(math.NaN())
	}
}

// handleDoorValues processes door states from Protobuf
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
				if kafkaError, ok := err.(kafka.Error); ok && kafkaError.Code() == kafka.ErrAllBrokersDown {
					log.Printf("Kafka broker is down: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				log.Printf("Error while consuming message: %v", err)
				continue
			}

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

			// Upload to S3 if enabled
			if service.S3Client != nil {
				if err := uploadToS3(service.S3Client, service.Config.AWS.Bucket, vehicleData.Vin, vehicleData); err != nil {
					log.Printf("Failed to upload vehicle data to S3: %v", err)
				}
			}

			// Backup locally if enabled
			if service.LocalBackupEnabled {
				if err := backupLocally(service.LocalBasePath, vehicleData.Vin, vehicleData); err != nil {
					log.Printf("Failed to backup vehicle data locally: %v", err)
				}
			}

			// Insert into ClickHouse if enabled
			if service.ClickHouseEnabled {
				if err := insertIntoClickHouse(service.ClickHouseClient, service.ClickHouseTableName, vehicleData); err != nil {
					log.Printf("Failed to insert vehicle data into ClickHouse: %v", err)
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
	// Parse command-line flags
	configPath := flag.String("config", "config.yaml", "Path to the YAML configuration file")
	promAddr := flag.String("prometheus.addr", ":2112", "Address for Prometheus metrics server")
	flag.Parse()

	// Load configuration
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
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
	go startPrometheusServer(*promAddr, &wg, ctx)

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

	// Close ClickHouse connection if initialized
	if service.ClickHouseEnabled {
		if err := service.ClickHouseClient.Close(); err != nil {
			log.Printf("Error closing ClickHouse connection: %v", err)
		} else {
			log.Println("ClickHouse connection closed successfully.")
		}
	}

	// Wait for all goroutines to finish
	wg.Wait()

	log.Println("Application shut down gracefully.")
}