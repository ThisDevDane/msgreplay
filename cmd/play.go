/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	rcd "github.com/ThisDevDane/msgreplay/recording"
	"github.com/spf13/cobra"
)

var (
	inputName     string
	instantReplay bool
)

// playCmd represents the play command
var playCmd = &cobra.Command{
	Use:   "play",
	Short: "Playback recording to RabbitMQ message broker",
	RunE:  playRun,
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
		rm := rcd.RecordedMessage{}
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

func init() {
	rootCmd.AddCommand(playCmd)

	playCmd.Flags().StringVarP(&inputName, "input", "i", "", "name of the recording to play")
	playCmd.MarkFlagRequired("input")

	playCmd.Flags().BoolVar(&instantReplay, "instant-replay", false, "don't wait between publishing messages for recorded offset and just publish as fast as possible")
}
