package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"regexp"
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
	queuesRegex    []*regexp.Regexp

	outputName    string
	recordingFile *recording.Recording
)

var recordCmd = &cobra.Command{
	Use:   "record",
	Short: "Duplicate specified queues and record message to local file",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		queuesRegex = []*regexp.Regexp{}
		for _, v := range queuesToRecord {
			r, err := regexp.Compile(v)
			if err != nil {
				return err
			}

			queuesRegex = append(queuesRegex, r)
		}

		return nil
	},
	RunE: recordRun,
}

func recordRun(cmd *cobra.Command, _ []string) error {
	log.Info().Msg("Setting up connection to RabbitMQ")
	client, err := rabbithole.NewClient(fmt.Sprintf("http://%s:%d", host, managementPort), username, password)
	if err != nil {
		return err
	}
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, amqpPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Info().Msgf("Setting up output file")
	recordingFile, err = recording.NewRecording(cmd.Context(), outputName, true)
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
	log.Info().Msg("Recording started...")

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

func (rd *RecordingDevice) SetupQueues(queuesToRecord []string) ([]QueueData, error) {
	result := []QueueData{}
	queues, _ := rd.ListQueuesIn(vhost)

	qs := []rabbithole.QueueInfo{}
	for _, qi := range queues {
		for _, r := range queuesRegex {
			if r.MatchString(qi.Name) {
				qs = append(qs, qi)
			}
		}
	}

	if len(qs) <= 0 {
		return nil, fmt.Errorf("no queues found for requested queues: '%v'", queuesToRecord)
	}

	for _, queue := range qs {
		bindings, _ := rd.ListQueueBindings(vhost, queue.Name)
		if len(bindings) <= 1 {
			log.Warn().Msgf("Not recording '%s' as there isn't any bindings to copy", queue.Name)
			continue
		}

		log.Info().Msgf("Setting up recording queue for '%s' with %d bindings", queue.Name, len(bindings)-1)

		ch, _ := rd.Channel()

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

			go func(qd QueueData, b rabbithole.BindingInfo) {
				qd.Channel.QueueBind(qd.Queue.Name, b.RoutingKey, b.Source, false, nil)

				consumerTag := fmt.Sprintf("msgreplay-%s", qd.Queue.Name)
				msgs, err := qd.Channel.Consume(qd.Queue.Name, consumerTag, true, true, false, false, nil)
				if err != nil {
					log.Err(err).
						Str("queue", qd.Queue.Name).
						Str("constumer_tag", consumerTag).
						Msg("Error when starting to consume queue")
				}

				for d := range msgs {
					log.Trace().Msg("message consumed")
					rm := recording.DeliveryToRecordedMessage(d)
					recordingFile.RecordMessage(rm)
				}
			}(qd, b)
		}
	}

}

func init() {
	rootCmd.AddCommand(recordCmd)

	recordCmd.Flags().StringSliceVarP(&queuesToRecord, "queue", "q", nil, `names of the queues you want to record
you can record multiple by either providing this argument multiple times or by using a comma-seperated list
    i.e --queue=\"foo,bar\"
you can also specify this as a regular expression to match multiple queues (uses golang's flavoured regex)
    i.e -q "foo.*" to match 'foo', 'fooTest' and 'fooBar'`)
	recordCmd.MarkFlagRequired("queue")

	recordCmd.Flags().IntVar(&managementPort, "management-port", 15672, "port of the RabbitMQ management interface")
	recordCmd.Flags().StringVar(&vhost, "vhost", "/", "vhost you want to access the queues from")

	recordCmd.Flags().StringVarP(&outputName, "output", "o", "", "name of the output sqlite file to store the recorded messages in")
}
