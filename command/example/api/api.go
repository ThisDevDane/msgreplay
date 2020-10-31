package api

import (
	"flag"
	"fmt"

	"github.com/michaelklishin/rabbit-hole/v2"
	mcli "github.com/mitchellh/cli"
)

type cmd struct {
	UI mcli.Ui

	//Flags
	flags *flag.FlagSet
	endpoint string
	username string
	password string
}

func New(ui mcli.Ui) *cmd {
	cmd := &cmd{UI: ui}

	cmd.flags = flag.NewFlagSet("", flag.ContinueOnError)
	cmd.flags.StringVar(&cmd.endpoint, "endpoint", "http://localhost:15672", "The base url and scheme for the rabbitMQ management interface")
	cmd.flags.StringVar(&cmd.username, "username", "guest", "The base url for the rabbitMQ management interface")
	cmd.flags.StringVar(&cmd.password, "password", "guest", "The base url for the rabbitMQ management interface")

	return cmd
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return help
}

type QueueInfo struct {
	Name string `json:name`
}


func (c *cmd) Run(args []string) int {
	if err := c.flags.Parse(args); err != nil {
		return 1
	}

	rmqc, _ := rabbithole.NewClient(c.endpoint, c.username, c.password)
	_, err := rmqc.Overview()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Couldn't connect to management API: %v", err))
		return 1
	}

	queues, _ := rmqc.ListQueues()
	bindings, _ := rmqc.ListQueueBindings(queues[0].Vhost, queues[0].Name)

	c.UI.Info(fmt.Sprintf("%+v", queues))
	c.UI.Info(fmt.Sprintf("%+v", bindings))

	return 0
}

const synopsis = "Run an example using the rabbitMQ API"
const help = `
Usage: msgreplay example api [options] [child...]
`