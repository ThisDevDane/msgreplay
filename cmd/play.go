/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
)

var (
	inputName     string
	instantReplay bool
)

// playCmd represents the play command
var playCmd = &cobra.Command{
	Use:   "play",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: playRun,
}

func playRun(_ *cobra.Command, _ []string) error {

	recording, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", inputName))
	if err != nil {
		return err
	}
	verRow := recording.QueryRow("SELECT * FROM version")
	version := "unknown"
	verRow.Scan(&version)
	if version != "v1" {
		return fmt.Errorf("version: %s is unsupported by this version of msgreplay", version)
	}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d", username, password, host, amqpPort))
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	rows, _ := recording.Query("SELECT * FROM messages")
	if err != nil {
		return err
	}
	for rows.Next() {
		rm := RecordedMessage{}
		blob := []byte{}

		err := rows.Scan(&rm.Offset, &rm.Exchange, &rm.RoutingKey,
			&blob,
			&rm.Pub.ContentType,
			&rm.Pub.ContentEncoding,
			&rm.Pub.DeliveryMode,
			&rm.Pub.CorrelationId,
			&rm.Pub.ReplyTo,
			&rm.Pub.Expiration,
			&rm.Pub.MessageId,
			&rm.Pub.Type,
			&rm.Pub.UserId,
			&rm.Pub.AppId,
			&rm.Pub.Body)

		json.Unmarshal(blob, &rm.Pub.Headers)
		if err != nil {
			log.Error().Err(err).Msg("Failed to scan message from recording")
			continue
		}

		go rm.Publish(ch)
	}

	return nil
}

type RecordedMessage struct {
	Offset float64

	Exchange   string
	RoutingKey string

	Pub amqp.Publishing
}

func DeliveryToRecordedMessage(d amqp.Delivery) RecordedMessage {
	rm := RecordedMessage{}
	rm.Exchange = d.Exchange
	rm.RoutingKey = d.RoutingKey

	rm.Pub.Headers = d.Headers
	rm.Pub.ContentType = d.RoutingKey
	rm.Pub.ContentEncoding = d.ContentEncoding
	rm.Pub.DeliveryMode = d.DeliveryMode
	rm.Pub.CorrelationId = d.CorrelationId
	rm.Pub.ReplyTo = d.ReplyTo
	rm.Pub.Expiration = d.Expiration
	rm.Pub.MessageId = d.MessageId
	rm.Pub.Type = d.Type
	rm.Pub.UserId = d.UserId
	rm.Pub.AppId = d.AppId
	rm.Pub.Body = d.Body

	return rm
}

func (rm RecordedMessage) Save(output *sql.DB, startTs time.Time) error {
	headerBlob, err := convertHeadersToBytes(rm.Pub.Headers)
	if err != nil {
		log.Error().
			Err(err).
			Interface("headers", rm.Pub.Headers).
			Msg("Unable to convert headers to []byte")
	}

	log.Info().Msg("Inserting msg into output file")

	_, err = output.Exec("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		time.Since(startTs).Seconds(),
		rm.Exchange,
		rm.RoutingKey,
		headerBlob,
		rm.Pub.ContentType,
		rm.Pub.ContentEncoding,
		rm.Pub.DeliveryMode,
		rm.Pub.CorrelationId,
		rm.Pub.ReplyTo,
		rm.Pub.Expiration,
		rm.Pub.MessageId,
		rm.Pub.Type,
		rm.Pub.UserId,
		rm.Pub.AppId,
		rm.Pub.Body)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Failerm.Pub to insert message to output file")
		return err
	}

	return nil
}

func (rm RecordedMessage) Publish(ch *amqp.Channel) error {
	log.Trace().
		Str("exchange", rm.Exchange).
		Str("routing_key", rm.RoutingKey).
		Int("body_len", len(rm.Pub.Body)).
		Int("header_len", len(rm.Pub.Headers)).
		Msg("publishing message")
	err := ch.Publish(rm.Exchange, rm.RoutingKey, false, false, rm.Pub)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish message from recording")
		return err
	}

	return nil
}

func init() {
	rootCmd.AddCommand(playCmd)

	playCmd.Flags().StringVarP(&inputName, "input", "i", "", "name of the recording to play")
	playCmd.MarkFlagRequired("input")

	playCmd.Flags().BoolVar(&instantReplay, "instant-replay", false, "don't wait between publishing messages for recorded offset and just publish as fast as possible")
}
