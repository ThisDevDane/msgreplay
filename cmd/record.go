package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThisDevDane/msgreplay/recording"
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
	recordingFile *recording.Recording
)

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "Duplicate specified queues and record message to local file",
	RunE:  recordRun,
}

func recordRun(_ *cobra.Command, _ []string) error {
	log.Info().Msg("Setting up connection to RabbitMQ")
	client, _ := rabbithole.NewClient(fmt.Sprintf("http://%s:%d", host, managementPort), username, password)
	conn, _ := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, amqpPort))
	defer conn.Close()

	log.Info().Msgf("Setting up output file")
	var err error
	recordingFile, err = recording.NewRecording(outputName, true)
	if err != nil {
		return err
	}
	defer recordingFile.Close()

	rd := RecordingDevice{client, conn}

	data, err := rd.SetupQueues(queuesToRecord)
	if err != nil {
		for _, qd := range data {
			qd.Channel.Close()
		}

		return err
	}

	rd.BindQueuesAndRecord(data)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	for _, qd := range data {
		qd.Channel.Close()
	}

	return nil
}

type RecordingDevice struct {
	*rabbithole.Client
	*amqp.Connection
}

type QueueData struct {
	Bindings []rabbithole.BindingInfo
	Queue    amqp.Queue
	Channel  *amqp.Channel
}

func (rd *RecordingDevice) SetupQueues(queues []string) ([]QueueData, error) {
	result := []QueueData{}

	for _, qtr := range queues {
		queue, err := rd.GetQueue(vhost, qtr)
		if err != nil {
			return nil, err
		}
		bindings, _ := rd.ListQueueBindings(vhost, queue.Name)
		if len(bindings) <= 1 {
			log.Warn().Msgf("Not recording '%s' as there isn't any bindings to copy", queue.Name)
			continue
		}

		log.Info().Msgf("Setting up recording queue for '%s' with %d bindings", queue.Name, len(bindings)-1)

		ch, _ := rd.Channel()
		defer ch.Close()

		q, err := ch.QueueDeclare(fmt.Sprintf("msgreplay-%s-%v", queue.Name, uuid.New().String()), false, true, true, false, nil)
		if err != nil {
			return nil, err
		}

		result = append(result, QueueData{bindings, q, ch})
	}

	return result, nil
}

func (rd *RecordingDevice) BindQueuesAndRecord(data []QueueData) {
	for _, qd := range data {
		for _, b := range qd.Bindings {
			if b.Source == "" { // avoid default exchange binding
				continue
			}

			go func(b rabbithole.BindingInfo) {
				qd.Channel.QueueBind(qd.Queue.Name, b.RoutingKey, b.Source, false, nil)

				msgs, _ := qd.Channel.Consume(qd.Queue.Name, "msgreplay", true, true, false, false, nil)

				for d := range msgs {
					rm := recording.DeliveryToRecordedMessage(d)
					recordingFile.RecordMessage(rm)
				}
			}(b)
		}
	}

}

func init() {
	rootCmd.AddCommand(recordCmd)

	recordCmd.Flags().StringSliceVarP(&queuesToRecord, "queue", "q", nil, "names of the queues you want to record\nYou can record multiple by either providing this argument multiple times or by using a comma-seperated list\n\ti.e --queue=\"foo,bar\"\n")
	recordCmd.MarkFlagRequired("queue")

	recordCmd.Flags().IntVar(&managementPort, "management-port", 15672, "port of the RabbitMQ management interface")
	recordCmd.Flags().StringVar(&vhost, "vhost", "/", "vhost you want to access the queues from")

	recordCmd.Flags().StringVarP(&outputName, "output", "o", "", "name of the output sqlite file to store the recorded messages in")
}
