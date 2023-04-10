# MsgReplay

![Docker Workflow Status](https://img.shields.io/github/actions/workflow/status/thisdevdane/msgreplay/docker.yaml?label=container)
![GitHub all releases](https://img.shields.io/github/downloads/thisdevdane/msgreplay/total)
![GitHub](https://img.shields.io/github/license/thisdevdane/msgreplay)

## Usage

```
❯ msgreplay
This program will consume message sent on specified queues by duplicating the queues and binding them to the same exchanges (will not work if sent directly to the queue name)
It will save this in a recording file 'my-recording.rec' which you can use this program to resend the message to the same or another RabbitMQ instance

Usage:
  msgreplay [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  generate    (WIP) Generate random fake message, useful for testing MsgReplay
  help        Help about any command
  play        Playback recording to RabbitMQ message broker
  record      Duplicate specified queues and record message to local file

Flags:
  -h, --help              help for msgreplay
      --host string       host of the rabbitmq instance to access from (default "localhost")
      --password string   password for authentication with rabbitmq (default "guest")
  -p, --port int          port of rabbitmq's amqp interface (default 5672)
      --username string   username for authentication with rabbitmq (default "guest")
  -v, --verbose           verbose output, useful for debugging

Use "msgreplay [command] --help" for more information about a command.
```
