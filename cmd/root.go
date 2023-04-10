package cmd

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	verbose bool

	host     string
	amqpPort int

	username string
	password string
)

var rootCmd = &cobra.Command{
	Use:   "msgreplay",
	Short: "Program to duplicate and save message sent to RabbitMQ queues",
	Long: `This program will consume message sent on specified queues by duplicating the queues and binding them to the same exchanges (will not work if sent directly to the queue name)
It will save this in a recording file 'my-recording.rec' which you can use this program to resend the message to the same or another RabbitMQ instance`,
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		if verbose {
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
		}
		return nil
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output, useful for debugging")

	rootCmd.PersistentFlags().StringVar(&host, "host", "localhost", "host of the rabbitmq instance to access from")
	rootCmd.PersistentFlags().IntVarP(&amqpPort, "port", "p", 5672, "port of rabbitmq's amqp interface")
	rootCmd.PersistentFlags().StringVar(&username, "username", "guest", "username for authentication with rabbitmq")
	rootCmd.PersistentFlags().StringVar(&password, "password", "guest", "password for authentication with rabbitmq")
}
