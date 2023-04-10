/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/ThisDevDane/msgreplay/recording"
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

func playRun(cmd *cobra.Command, _ []string) error {
	recording, err := recording.OpenRecording(cmd.Context(), inputName)
	if err != nil {
		return err
	}
	defer recording.Close()

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

	start := time.Now().UTC()
	msgCh, err := recording.PlayRecording()
	if err != nil {
		return err
	}

	for rm := range msgCh {
		if !instantReplay {
			since := time.Since(start)
			offsetDur := time.Duration(rm.Offset) * time.Second
			if since < offsetDur {
				time.Sleep((since - offsetDur).Abs())
			}

		}
		log.Info().
			Str("exchange", rm.Exchange).
			Str("routing_key", rm.RoutingKey).
			Float64("offset", rm.Offset).
			Msg("Publishing message")
		rm.Publish(ch)
	}

	log.Trace().Msg("end of program")
	return nil
}

func init() {
	rootCmd.AddCommand(playCmd)

	playCmd.Flags().StringVarP(&inputName, "input", "i", "", "name of the recording to play")
	playCmd.MarkFlagRequired("input")

	playCmd.Flags().BoolVar(&instantReplay, "instant-replay", false, "don't wait between publishing messages for recorded offset and just publish as fast as possible")
}
