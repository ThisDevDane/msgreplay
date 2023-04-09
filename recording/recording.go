package recording

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

type Recording struct {
	db *sql.DB

	startOfRecording time.Time
}

func NewRecording(name string, startTimeImmediatly bool) (*Recording, error) {
	db, err := sql.Open("sqlite3", recordingDSN(name))
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
	if startTimeImmediatly {
		rec.StartRecordingTime()
	}

	return rec, nil
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

	_, err = r.db.Exec("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

func (r *Recording) Close() {
	r.db.Close()
}

func recordingDSN(outputName string) string {
	if outputName == "" {
		outputName = fmt.Sprintf("recording-%v", time.Now().UTC().Format(time.RFC3339))
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
