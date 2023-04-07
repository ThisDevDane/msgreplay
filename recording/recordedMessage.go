package recording

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

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

func convertHeadersToBytes(headers amqp.Table) ([]byte, error) {
	buf, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("failed to encode headers using json: %w", err)
	}

	return buf, nil
}
