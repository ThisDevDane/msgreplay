package amqp

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	mcli "github.com/mitchellh/cli"
	"github.com/streadway/amqp"
)

const synopsis = "Run an example AMQP on a rabbitMQ instance"
const help = `
Usage: msgreplay example [options] [child...]
`

var (
	wg sync.WaitGroup
)

type cmd struct {
	UI mcli.Ui
}

func New(ui mcli.Ui) *cmd {
	return &cmd{UI: ui}
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return help
}

func (c *cmd) Run(args []string) int {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	c.failOnError(err, "Failed to connect to rabbitMQ")
	go c.notifyConnThrottled(conn.NotifyBlocked(make(chan amqp.Blocking)))
	go c.notifyConnClosed(conn.NotifyClose(make(chan *amqp.Error)))
	ctx, cancelCtx := context.WithCancel(context.Background())

	defer func() {
		err := conn.Close()
		if err != nil {
			c.UI.Info(fmt.Sprintf("Error trying to close rabbitMQ connection: %s\n Treated as closed anyway.", err))
		}
	}()

	c.UI.Info("Connected to rabbitMQ....")

	queue := c.setupTestQueue(conn)

	go c.publishMessages(conn, queue.Name, ctx)
	go c.consumeMessage(conn, queue.Name, ctx)

	c.UI.Info("Press Ctrl+C to stop execution...")
	sigc := make(chan os.Signal)
	signal.Notify(sigc, os.Interrupt)
	<-sigc

	c.UI.Info(fmt.Sprintf("Ending execution"))

	cancelCtx()
	wg.Wait()

	c.UI.Info("Goodbye!")

	return 0
}

func (c *cmd) setupTestQueue(conn *amqp.Connection) amqp.Queue {
	ch, err := conn.Channel()
	c.failOnError(err, "Unable to open a channel to rabbitMQ")

	queue, err := ch.QueueDeclare(
		"test", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	c.failOnError(err, "Failed to declare a queue")
	ch.Close()
	return queue
}

func (c *cmd) publishMessages(conn *amqp.Connection, name string, ctx context.Context) {
	wg.Add(1)
	defer wg.Done()
	publishChannel, err := conn.Channel()
	c.failOnError(err, "Unable to open a channel to rabbitMQ")
	defer publishChannel.Close()
	c.UI.Info("Publishing started")
top:
	for {
		select {
		case <-ctx.Done():
			c.UI.Info("Shutting down publishing...")
			break top
		default:
			err = publishChannel.Publish(
				"",    // exchange
				name,  // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte("hello world"),
				})
			if err != nil {
				c.UI.Info(fmt.Sprintf("Failed to publish a message: %v\n", err))
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.UI.Info("Shut down publishing")
}

func (c *cmd) consumeMessage(conn *amqp.Connection, name string, ctx context.Context) {
	wg.Add(1)
	defer wg.Done()
	consumeChannel, err := conn.Channel()
	c.failOnError(err, "Unable to open a channel to rabbitMQ")
	msgs, err := consumeChannel.Consume(name, "", false, false, false, false, nil)
	c.failOnError(err, "Couldn't open consume channel")
	c.UI.Info("Consumption started")

	go func() {
		for m := range msgs {
			err := m.Ack(false)
			if err != nil {
				c.UI.Info(fmt.Sprintf("Unable to ACK message(%v): %v\n", m.DeliveryTag, err))
			}
		}
	}()

	<-ctx.Done()
	c.UI.Info("Shutting down consuming...")
	consumeChannel.Close()
	c.UI.Info("Shut down consumption")
}

func (c *cmd) notifyConnClosed(channel chan *amqp.Error) {
	for e := range channel {
		initiator := "server"
		if e.Server == false {
			initiator = "library"
		}
		c.UI.Info(fmt.Sprintf("rabbitMQ connection closed(%v) by %s: \"%v\", can be recovered; %v\n",
			e.Code,
			initiator,
			e.Reason,
			e.Recover))
	}
}

func (c *cmd) notifyConnThrottled(channel chan amqp.Blocking) {
	for e := range channel {
		if e.Active {
			c.UI.Info(fmt.Sprintf("rabbitMQ connection getting throttled: %s\n", e.Reason))
		}
	}
}

func (c *cmd) failOnError(err error, msg string) {
	if err != nil {
		c.UI.Error(fmt.Sprintf("%s: %s", msg, err))
		os.Exit(1)
	}
}
