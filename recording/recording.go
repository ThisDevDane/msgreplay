package recording

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

type Recording struct {
	db  *sql.DB
	ctx context.Context

	startOfRecording time.Time
}

func NewRecording(ctx context.Context, name string, startTimeImmediatly bool) (*Recording, error) {
	db, err := sql.Open("sqlite3", newRecordingDSN(name))
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createVerTblStmt)
	if err != nil {
		return nil, err
	}
	db.Exec("INSERT INTO version VALUES ('v1')")

	_, err = db.Exec(createMsgTblStmt)
	if err != nil {
		return nil, err
	}

	rec := new(Recording)
	rec.db = db
	rec.ctx = ctx
	if startTimeImmediatly {
		rec.StartRecordingTime()
	}

	return rec, nil
}

func OpenRecording(ctx context.Context, name string) (*Recording, error) {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return nil, err
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", name))
	if err != nil {
		return nil, err
	}
	verRow := db.QueryRow("SELECT * FROM version")
	version := "unknown"
	verRow.Scan(&version)
	if version != "v1" {
		return nil, fmt.Errorf("version: %s is unsupported by this version of msgreplay", version)
	}

	return &Recording{
		db,
		ctx,
		time.Time{},
	}, nil
}

func (r *Recording) StartRecordingTime() {
	r.startOfRecording = time.Now().UTC()
}

func (r *Recording) RecordMessage(msg RecordedMessage) error {
	headerBlob, err := convertHeadersToBytes(msg.Pub.Headers)
	if err != nil {
		log.Error().
			Err(err).
			Interface("headers", msg.Pub.Headers).
			Msg("Unable to convert headers to []byte")
	}

	log.Info().Msg("Inserting msg into output file")

	_, err = r.db.ExecContext(r.ctx, "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		time.Since(r.startOfRecording).Seconds(),
		msg.Exchange,
		msg.RoutingKey,
		headerBlob,
		msg.Pub.ContentType,
		msg.Pub.ContentEncoding,
		msg.Pub.DeliveryMode,
		msg.Pub.CorrelationId,
		msg.Pub.ReplyTo,
		msg.Pub.Expiration,
		msg.Pub.MessageId,
		msg.Pub.Type,
		msg.Pub.UserId,
		msg.Pub.AppId,
		msg.Pub.Body)

	if err != nil {
		log.Error().
			Err(err).
			Msg("Failemsg.Pub to insert message to output file")
		return err
	}

	return nil
}

func (r *Recording) PlayRecording() (<-chan RecordedMessage, error) {
	channel := make(chan RecordedMessage, 5)
	rows, err := r.db.QueryContext(r.ctx, "SELECT * FROM messages")
	if err != nil {
		return nil, err
	}
	go func(ch chan<- RecordedMessage) {
		defer rows.Close()
		c := 0
		for rows.Next() {
			c += 1
			log.Trace().Int("count", c).Msg("scanning recorded message")
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

			if err != nil {
				log.Error().Err(err).Msg("Failed to scan message from recording")
				continue
			}

			json.Unmarshal(blob, &rm.Pub.Headers)
			rm.ctx = r.ctx

			ch <- rm
		}

		err := rows.Err()
		if err != nil {
			log.Error().Err(err).Msg("Failed to iterate messages from recording")
		}

		close(ch)
	}(channel)

	return channel, nil
}

func (r *Recording) Close() {
	r.db.Close()
}

func newRecordingDSN(outputName string) string {
	if outputName == "" {
		outputName = fmt.Sprintf("recording-%v.rec", time.Now().UTC().Format(time.RFC3339))
	}

	if _, err := os.Stat(outputName); !os.IsNotExist(err) {
		os.Remove(outputName)
	}

	return fmt.Sprintf("file:%s", outputName)
}

const createVerTblStmt = `CREATE TABLE version
(
    version TEXT
)`

const createMsgTblStmt = `CREATE TABLE messages
(
    start_offset    REAL,
    exchange        TEXT,
    routingKey      TEXT,

    headers         BLOB,

    contentType     TEXT,
    contentEncoding TEXT,
    deliveryMode    INTEGER,
    correlationId   TEXT,
    replyTo         TEXT,
    expiration      TEXT,
    messageId       TEXT,
    type            TEXT,
    userid          TEXT,
    appid           TEXT,

    body            BLOB
)`
