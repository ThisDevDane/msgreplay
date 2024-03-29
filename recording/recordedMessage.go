package recording

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

type RecordedMessage struct {
	Offset float64

	Exchange   string
	RoutingKey string

	Pub amqp.Publishing

	ctx context.Context
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
	rm.ctx = context.Background()

	return rm
}

func (rm RecordedMessage) Publish(ch *amqp.Channel) error {
	err := ch.PublishWithContext(rm.ctx, rm.Exchange, rm.RoutingKey, false, false, rm.Pub)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish message from recording")
		return err
	}

	return nil
}

func convertHeadersToBytes(headers amqp.Table) ([]byte, error) {
	buf, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("failed to encode headers using json: %w", err)
	}

	return buf, nil
}
