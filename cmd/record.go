package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"database/sql"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	rabbithole "github.com/michaelklishin/rabbit-hole/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	managementPort int
	vhost          string

	queuesToRecord []string

	outputName    string
	recordingFile *sql.DB
)

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: recordRun,
}

func recordRun(_ *cobra.Command, _ []string) error {
	log.Info().Msg("Setting up connection to RabbitMQ")
	client, _ := rabbithole.NewClient(fmt.Sprintf("http://%s:%d", host, managementPort), username, password)
	conn, _ := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, amqpPort))
	defer conn.Close()

	log.Info().Msgf("Setting up output file")
	var err error
	recordingFile, err = setupRecordingFile(outputName)
	if err != nil {
		return err
	}
	defer recordingFile.Close()

	startTs := time.Now().UTC()
	for _, qtr := range queuesToRecord {
		queue, err := client.GetQueue(vhost, qtr)
		if err != nil {
			return err
		}
		bindings, _ := client.ListQueueBindings(vhost, queue.Name)
		if len(bindings) <= 1 {
			log.Warn().Msgf("Not recording '%s' as there isn't any bindings to copy", queue.Name)
			continue
		}

		log.Info().Msgf("Setting up recording queue for '%s' with %d bindings", queue.Name, len(bindings)-1)

		ch, _ := conn.Channel()
		defer ch.Close()

		q, err := ch.QueueDeclare(fmt.Sprintf("msgreplay-%s-%v", queue.Name, uuid.New().String()), false, true, true, false, nil)
		if err != nil {
			return err
		}

		for _, b := range bindings {
			if b.Source == "" { // avoid default exchange binding
				continue
			}

			ch.QueueBind(q.Name, b.RoutingKey, b.Source, false, nil)

			msgs, _ := ch.Consume(q.Name, "msgreplay", true, true, false, false, nil)

			go func() {
				for d := range msgs {
					rm := DeliveryToRecordedMessage(d)
					rm.Save(recordingFile, startTs)
				}
			}()
		}
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	return nil
}

func setupRecordingFile(outputName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", createOutputFileDSN(outputName))
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createVerTblStmt)
	if err != nil {
		return nil, err
	}
	db.Exec("INSERT INTO version VALUES ('v1')")

	_, err = db.Exec(createMsgTblStmt)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func createOutputFileDSN(outputName string) string {
	if outputName == "" {
		outputName = fmt.Sprintf("recording-%v", time.Now().UTC().Format(time.RFC3339))
	}

	if _, err := os.Stat(outputName); !os.IsNotExist(err) {
		os.Remove(outputName)
	}

	return fmt.Sprintf("file:%s", outputName)
}

func convertHeadersToBytes(headers amqp.Table) ([]byte, error) {
	buf, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("failed to encode headers using json: %w", err)
	}

	return buf, nil
}

func init() {
	rootCmd.AddCommand(recordCmd)

	recordCmd.Flags().StringSliceVarP(&queuesToRecord, "queue", "q", nil, "names of the queues you want to record\nYou can record multiple by either providing this argument multiple times or by using a comma-seperated list\n\ti.e --queue=\"foo,bar\"\n")
	recordCmd.MarkFlagRequired("queue")

	recordCmd.Flags().IntVar(&managementPort, "management-port", 15672, "port of the RabbitMQ management interface")
	recordCmd.Flags().StringVar(&vhost, "vhost", "/", "vhost you want to access the queues from")

	recordCmd.Flags().StringVarP(&outputName, "output", "o", "", "name of the output sqlite file to store the recorded messages in")
}

const createVerTblStmt = `CREATE TABLE version
(
    version TEXT
)`

const createMsgTblStmt = `CREATE TABLE messages
(
    start_offset    REAL,
    exchange        TEXT,
    routingKey      TEXT,

    headers         BLOB,

    contentType     TEXT,
    contentEncoding TEXT,
    deliveryMode    INTEGER,
    correlationId   TEXT,
    replyTo         TEXT,
    expiration      TEXT,
    messageId       TEXT,
    type            TEXT,
    userid          TEXT,
    appid           TEXT,

    body            BLOB
)`
