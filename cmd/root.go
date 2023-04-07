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
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
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
