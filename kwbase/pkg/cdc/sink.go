//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package cdc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	gojson "encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/Shopify/sarama"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

// Constants for the sink options.
const (
	SinkSchemeKafka            = `kafka`
	SinkSchemeMock             = `mock`
	SinkParameterSchemaTopic   = `schema_topic`
	SinkParameterTLSEnabled    = `tls_enabled`
	SinkParameterTopic         = `topic_name`
	SinkParameterCACert        = `ca_cert`
	SinkParameterClientCert    = `client_cert`
	SinkParameterClientKey     = `client_key`
	SinkParameterSASLEnabled   = `sasl_enabled`
	SinkParameterSASLHandshake = `sasl_handshake`
	SinkParameterSASLUser      = `sasl_user`
	SinkParameterSASLPassword  = `sasl_password`
	SinkParameterMockToDB      = `mock_sink_to_db`
)

// TsPipeSinkMaxRetries indicates the retry times of Sink.
var TsPipeSinkMaxRetries = func() *settings.IntSetting {
	s := settings.RegisterNonNegativeIntSetting(
		"ts.pipe.sink_max_retries",
		"the max retry times of pipe sink",
		5,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// Sink is an abstraction for anything that a pipe may send into.
type Sink interface {
	// Send sends messages for asynchronous delivery on the sink.
	Send(ctx context.Context, key string, value []byte) error

	// SendWithFormat sends messages with the specified format for asynchronous delivery on the sink.
	SendWithFormat(ctx context.Context, format cdcpb.FormatType, value []byte) error

	// SendHighWater sends high-water message to sink.
	SendHighWater(ctx context.Context, highWater int64) error

	// Flush messages until every message enqueued by Send. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error

	// Close the sink, it does not guarantee delivery of outstanding messages.
	Close() error
}

// CheckSlink checks if sinkURI valid.
func CheckSlink(sinkURI string, enabled bool) error {
	sink, err := CreateSink(
		nil, sinkURI, int(TsPipeSinkMaxRetries.Default()), 0, !enabled,
	)
	if err != nil {
		return err
	}

	if !enabled {
		return nil
	}

	return sink.Close()
}

// CreateSink create a sink instance from sinkURI.
func CreateSink(
	_ context.Context, sinkURI string, maxRetries int, maxMessageBytes int, verifyOnly bool,
) (Sink, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return nil, err
	}
	q := u.Query()

	var newSink func() (Sink, error)
	switch {
	case u.Scheme == SinkSchemeKafka:
		cfg, err := parseKafkaConfig(q)
		if err != nil {
			return nil, err
		}

		cfg.maxRetries = maxRetries
		cfg.maxMessageBytes = maxMessageBytes
		newSink = func() (Sink, error) {
			return newKafkaSink(cfg, u.Host)
		}
	case u.Scheme == SinkSchemeMock:
		cfg, err := parseKafkaConfig(q)
		if err != nil {
			return nil, err
		}

		newSink = func() (Sink, error) {
			return newMockSink(cfg.topic, sinkURI)
		}
	default:
		return nil, errors.Errorf("Sink schema %q is not yet supported", u.Scheme)
	}

	if verifyOnly {
		return nil, nil
	}

	s, err := newSink()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func parseKafkaConfig(q url.Values) (*kafkaSinkConfig, error) {
	cfg := &kafkaSinkConfig{}
	var err error
	cfg.topic = q.Get(SinkParameterTopic)
	if cfg.topic == "" {
		return nil, errors.Errorf(`%s is empty`, SinkParameterTopic)
	}
	q.Del(SinkParameterTopic)
	if schemaTopic := q.Get(SinkParameterSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, SinkParameterSchemaTopic)
	}
	q.Del(SinkParameterSchemaTopic)
	if tlsBool := q.Get(SinkParameterTLSEnabled); tlsBool != `` {
		var err error
		if cfg.tlsEnabled, err = strconv.ParseBool(tlsBool); err != nil {
			return nil, errors.Errorf(`Parameter %s must be a bool: %s`, SinkParameterTLSEnabled, err)
		}
	}
	q.Del(SinkParameterTLSEnabled)
	if caCertHex := q.Get(SinkParameterCACert); caCertHex != `` {
		if cfg.caCert, err = base64.StdEncoding.DecodeString(caCertHex); err != nil {
			return nil, errors.Errorf(`Parameter %s must be base 64 encoded: %s`, SinkParameterCACert, err)
		}
	}
	q.Del(SinkParameterCACert)
	if clientCertHex := q.Get(SinkParameterClientCert); clientCertHex != `` {
		if cfg.clientCert, err = base64.StdEncoding.DecodeString(clientCertHex); err != nil {
			return nil, errors.Errorf(`Parameter %s must be base 64 encoded: %s`, SinkParameterClientCert, err)
		}
	}
	q.Del(SinkParameterClientCert)
	if clientKeyHex := q.Get(SinkParameterClientKey); clientKeyHex != `` {
		if cfg.clientKey, err = base64.StdEncoding.DecodeString(clientKeyHex); err != nil {
			return nil, errors.Errorf(`Parameter %s must be base 64 encoded: %s`, SinkParameterClientKey, err)
		}
	}
	q.Del(SinkParameterClientKey)

	saslParameter := q.Get(SinkParameterSASLEnabled)
	q.Del(SinkParameterSASLEnabled)
	if saslParameter != `` {
		b, err := strconv.ParseBool(saslParameter)
		if err != nil {
			return nil, errors.Wrapf(err, `Parameter %s must be a bool:`, SinkParameterSASLEnabled)
		}
		cfg.saslEnabled = b
	}
	handshakeParameter := q.Get(SinkParameterSASLHandshake)
	q.Del(SinkParameterSASLHandshake)
	if handshakeParameter == `` {
		cfg.saslHandshake = true
	} else {
		if !cfg.saslEnabled {
			return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, SinkParameterSASLEnabled)
		}
		b, err := strconv.ParseBool(handshakeParameter)
		if err != nil {
			return nil, errors.Wrapf(err, `Parameter %s must be a bool:`, SinkParameterSASLHandshake)
		}
		cfg.saslHandshake = b
	}
	cfg.saslUser = q.Get(SinkParameterSASLUser)
	q.Del(SinkParameterSASLUser)
	cfg.saslPassword = q.Get(SinkParameterSASLPassword)
	q.Del(SinkParameterSASLPassword)
	if cfg.saslEnabled {
		if cfg.saslUser == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, SinkParameterSASLUser)
		}
		if cfg.saslPassword == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, SinkParameterSASLPassword)
		}
	} else {
		if cfg.saslUser != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, SinkParameterSASLEnabled)
		}
		if cfg.saslPassword != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, SinkParameterSASLEnabled)
		}
	}

	return cfg, nil
}

type kafkaSinkConfig struct {
	topic           string
	tlsEnabled      bool
	caCert          []byte
	clientCert      []byte
	clientKey       []byte
	saslEnabled     bool
	saslHandshake   bool
	saslUser        string
	saslPassword    string
	maxRetries      int
	maxMessageBytes int
}

type kafkaMessagePusher struct {
	cfg                 *kafkaSinkConfig
	client              sarama.Client
	producer            sarama.AsyncProducer
	lastMetadataRefresh time.Time

	stopCh    chan struct{}
	waitGroup sync.WaitGroup

	lock          syncutil.Mutex
	inflightCount int64
	flushErr      error
	flushCh       chan struct{}
}

var _ Sink = &kafkaMessagePusher{}

// Send implements the Sink interface
func (k *kafkaMessagePusher) Send(ctx context.Context, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: k.cfg.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	return k.sendMessage(ctx, msg)
}

// SendWithFormat implements the Sink interface
func (k *kafkaMessagePusher) SendWithFormat(
	ctx context.Context, _ cdcpb.FormatType, value []byte,
) error {
	return k.Send(ctx, timeutil.Now().String(), value)
}

// SendHighWater implements the Sink interface
func (k *kafkaMessagePusher) SendHighWater(ctx context.Context, highWater int64) error {
	if timeutil.Since(k.lastMetadataRefresh) > time.Minute {
		if err := k.client.RefreshMetadata(k.cfg.topic); err != nil {
			return err
		}
		k.lastMetadataRefresh = timeutil.Now()
	}

	msg := &sarama.ProducerMessage{
		Topic: k.cfg.topic,
		Key:   nil,
		Value: sarama.ByteEncoder(strconv.FormatInt(highWater, 10)),
	}
	if err := k.sendMessage(ctx, msg); err != nil {
		return err
	}

	return nil
}

// Flush implements the Sink interface
func (k *kafkaMessagePusher) Flush(ctx context.Context) error {
	flushCh := make(chan struct{}, 1)

	k.lock.Lock()
	inflightCount := k.inflightCount
	flushErr := k.flushErr
	k.flushErr = nil
	immediateFlush := inflightCount == 0 || flushErr != nil
	if !immediateFlush {
		k.flushCh = flushCh
	}
	k.lock.Unlock()

	if immediateFlush {
		return flushErr
	}

	if log.V(1) {
		log.Infof(ctx, "flush waiting for %d inflight messages", inflightCount)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flushCh:
		k.lock.Lock()
		flushErr := k.flushErr
		k.flushErr = nil
		k.lock.Unlock()
		return flushErr
	}
}

// Close implements the Sink interface
func (k *kafkaMessagePusher) Close() error {
	if k.stopCh == nil {
		return nil
	}

	close(k.stopCh)
	k.waitGroup.Wait()
	_ = k.producer.Close()
	if k.client != nil {
		return k.client.Close()
	}

	return nil
}

func (k *kafkaMessagePusher) start() {
	k.stopCh = make(chan struct{})
	k.waitGroup.Add(1)
	go k.waitSendResults()
}

func (k *kafkaMessagePusher) sendMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	k.lock.Lock()
	k.inflightCount++
	k.lock.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case k.producer.Input() <- msg:
	}

	return nil
}

func (k *kafkaMessagePusher) waitSendResults() {
	defer k.waitGroup.Done()

	for {
		select {
		case <-k.stopCh:
			return
		case <-k.producer.Successes():
		case err := <-k.producer.Errors():
			k.lock.Lock()
			if k.flushErr == nil {
				k.flushErr = err
			}
			k.lock.Unlock()
		}

		k.lock.Lock()
		k.inflightCount--
		if k.inflightCount == 0 && k.flushCh != nil {
			k.flushCh <- struct{}{}
			k.flushCh = nil
		}
		k.lock.Unlock()
	}
}

func newKafkaSink(cfg *kafkaSinkConfig, serverAddr string) (Sink, error) {
	sink := &kafkaMessagePusher{cfg: cfg}

	config := sarama.NewConfig()
	config.ClientID = `KaiwuDB`
	config.Net.DialTimeout = 500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = cfg.maxRetries
	config.Producer.Retry.Backoff = 2 * time.Second

	if cfg.caCert != nil {
		if !cfg.tlsEnabled {
			return nil, errors.Errorf(`%s requires %s=true`, SinkParameterCACert, SinkParameterTLSEnabled)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cfg.caCert)
		config.Net.TLS.Config = &tls.Config{
			RootCAs: caCertPool,
		}
		config.Net.TLS.Enable = true
	} else if cfg.tlsEnabled {
		config.Net.TLS.Enable = true
	}

	if cfg.clientCert != nil {
		if !cfg.tlsEnabled {
			return nil, errors.Errorf(`%s requires %s=true`, SinkParameterClientCert, SinkParameterTLSEnabled)
		}
		if cfg.clientKey == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, SinkParameterClientCert, SinkParameterClientKey)
		}
		cert, err := tls.X509KeyPair(cfg.clientCert, cfg.clientKey)
		if err != nil {
			return nil, errors.Errorf(`invalid client certificate data provided: %s`, err)
		}
		if config.Net.TLS.Config == nil {
			config.Net.TLS.Config = &tls.Config{}
		}
		config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
	} else if cfg.clientKey != nil {
		return nil, errors.Errorf(`%s requires %s to be set`, SinkParameterClientKey, SinkParameterClientCert)
	}

	if cfg.saslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = cfg.saslHandshake
		config.Net.SASL.User = cfg.saslUser
		config.Net.SASL.Password = cfg.saslPassword
	}

	config.Producer.Flush.Messages = 1
	config.Producer.Flush.MaxMessages = 1000
	config.Producer.Flush.Frequency = time.Hour
	// Producer.MaxMessageBytes is set to 1G, which is larger than the Kafka server's default of 1MB,
	// ensuring no limitation on the producer side.
	config.Producer.MaxMessageBytes = 1024 * 1024 * 1024

	var err error
	sink.client, err = sarama.NewClient(strings.Split(serverAddr, `,`), config)
	if err != nil {
		err = pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, serverAddr)
		return nil, err
	}

	sink.producer, err = sarama.NewAsyncProducerFromClient(sink.client)
	if err != nil {
		err = pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, serverAddr)
		return nil, err
	}

	sink.start()
	return sink, nil
}

// MockSink used to test.
type MockSink struct {
	table     string
	db        *pgx.Conn
	topic     string
	highWater int64
	mu        syncutil.Mutex
}

// SendWithFormat implements the Sink interface
func (m *MockSink) SendWithFormat(ctx context.Context, _ cdcpb.FormatType, value []byte) error {
	return m.Send(ctx, "", value)
}

// Send implements the Sink interface
func (m *MockSink) Send(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var msg MessageFormat
	err := gojson.Unmarshal(value, &msg)
	if err != nil {
		return err
	}

	if strings.Contains(msg.Table, "kafka_error") {
		return errors.Errorf(`Failed to produce message to topic %s: 
kafka server: Message was too large, server rejected it to avoid allocation error.`,
			m.topic,
		)
	}

	if strings.Contains(msg.Table, "error") {
		return errors.Errorf(`Failed to send data to topic %s.`,
			m.topic,
		)
	}

	if m.db != nil {
		if msg.Statement != "" {
			if _, err := m.db.Exec(
				fmt.Sprintf("insert into %s values ($1,$2,$3,$4,$5,$6)", m.table),
				m.topic, msg.Kind, msg.Database, msg.Table, timeutil.Now().UnixMilli(), value,
			); err != nil {
				return err
			}
		} else if len(msg.ColumnValues) > 0 {

			for _, row := range msg.ColumnValues {
				var ts int64

				if strings.Contains(msg.ColumnTypes[0], "TIMESTAMP") {
					ts = int64(row[0].(float64))
				} else {
					ts = timeutil.Now().UnixMilli()
				}
				rowVal, _ := gojson.Marshal(row)
				if _, err := m.db.Exec(
					fmt.Sprintf("insert into %s values ($1,$2,$3,$4,$5,$6)", m.table),
					m.topic, msg.Kind, msg.Database, msg.Table, ts, rowVal,
				); err != nil {
					return err
				}
			}
		}

		return nil
	}

	// fvt case
	if MockData == nil {
		fmt.Println(m.topic, key, string(value))
		return nil
	}

	// ut case
	if _, ok := MockData[m.topic]; !ok {
		MockData[m.topic] = [][]byte{}
	}

	data := make([]byte, len(value))
	copy(data, value)
	MockData[m.topic] = append(MockData[m.topic], data)

	return nil
}

// SendHighWater implements the Sink interface.
func (m *MockSink) SendHighWater(_ context.Context, highWater int64) error {
	m.highWater = highWater
	return nil
}

// Flush implements the Sink interface.
func (m *MockSink) Flush(_ context.Context) error {

	return nil
}

// Close implements the Sink interface.
func (m *MockSink) Close() error {
	if m.db != nil {
		err := m.db.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

var _ Sink = &MockSink{}

// MockData used to test.
var MockData map[string][][]byte

func newMockSink(topic string, uri string) (Sink, error) {
	sink := &MockSink{
		topic: topic,
	}

	if MockData == nil && cdcpb.MockData != nil {
		MockData = cdcpb.MockData
	}

	if MockData != nil {
		if _, ok := MockData[topic]; !ok {
			MockData[topic] = [][]byte{}
		}
	}

	if strings.Contains(uri, SinkParameterMockToDB) {
		u, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}

		q := u.Query()
		sink.table = q.Get(SinkParameterMockToDB)
		cfg, err := pgx.ParseURI(strings.ReplaceAll(uri, "mock://", "postgresql://"))
		if err != nil {
			return nil, err
		}

		sink.mu.Lock()
		defer sink.mu.Unlock()

		sink.db, err = pgx.Connect(cfg)
		if err != nil {
			return nil, err
		}
	}

	return sink, nil
}
