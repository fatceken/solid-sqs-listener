package listener

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/fatceken/solid-sqs-listener/listener/internal"
)

// New initializes a new Listener instance with given settings
func New(ctx context.Context, listenerSettings *Settings) (*Listener, error) {

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		lo.Region = listenerSettings.Region
		return nil
	})

	if err != nil {
		return nil, err
	}

	client := sqs.NewFromConfig(cfg)

	if err != nil {
		return nil, err
	}

	queueUrl, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(listenerSettings.QueueName),
	})

	if err != nil {
		return nil, err
	}

	listenerSettings.queueUrl = queueUrl.QueueUrl

	list := &Listener{
		context:             ctx,
		client:              client,
		settings:            listenerSettings,
		incomingMessageChan: make(chan types.Message),
		sig:                 make(chan os.Signal, 1),
	}

	signal.Notify(list.sig, os.Interrupt, syscall.SIGTERM)

	return list, nil
}

// Listen polls queue until SIGTERM or Interrupt signal received
func (l *Listener) Listen(consumer Consumer) error {

	go l.pollMessages() //producer

	go l.welcomeMessages(consumer) //consumer

	_, cancel := context.WithCancel(l.context)

	<-l.sig

	fmt.Println("shutting down")

	cancel()

	time.Sleep(l.settings.ShutDownGracePeriod)

	return nil
}

func (l *Listener) pollMessages() error {

	defer close(l.incomingMessageChan)

	for {
		select {
		case <-l.context.Done():
			return l.context.Err()
		case <-time.After(l.settings.Delay):
			input := &sqs.ReceiveMessageInput{
				QueueUrl: l.settings.queueUrl,
				AttributeNames: []types.QueueAttributeName{
					"SentTimestamp",
				},
				MaxNumberOfMessages: 1,
				MessageAttributeNames: []string{
					"All",
				},
				WaitTimeSeconds: int32(l.settings.WaitTime),
			}

			resp, err := l.client.ReceiveMessage(l.context, input)

			if err != nil {
				fmt.Println("Unable to receive message from queue ", err)
				return err
			} else {
				for _, msg := range resp.Messages {
					l.incomingMessageChan <- msg
				}
			}
		}
	}
}

func (l *Listener) welcomeMessages(consumer Consumer) error {

	for msg := range l.incomingMessageChan {

		err := l.performConsume(msg, consumer)
		if err != nil {
			fmt.Println("Error while consuming", err)
		} else {
			err = l.sendAck(msg)
			if err != nil {
				fmt.Println("Error while sending ack", err)
			}
		}
	}
	return nil
}

func (l *Listener) performConsume(msg types.Message, consumer Consumer) error {

	err := backoff.Retry(func() error {
		if err := consumer.Consume(msg); err != nil {
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(l.settings.MaxRetryCount)))

	if err != nil {
		return fmt.Errorf("messageId: %s couldn't consumed. %w", *msg.MessageId, err)
	}

	return nil
}

func (l *Listener) sendAck(msg types.Message) error {
	_, err := l.client.DeleteMessage(l.context, &sqs.DeleteMessageInput{
		QueueUrl:      l.settings.queueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		return fmt.Errorf("messageId: %s consumed but couldn't deleted from the queue. %w", *msg.MessageId, err)
	}

	return nil
}

// Settings holds config parameters
type Settings struct {
	//Name of the queue
	QueueName string
	//
	WaitTime            int
	Region              string
	Delay               time.Duration
	MaxRetryCount       int
	ShutDownGracePeriod time.Duration
	queueUrl            *string
}

type Consumer interface {
	Consume(types.Message) error
}

type Listener struct {
	context             context.Context
	client              internal.SqsApiFace
	settings            *Settings
	incomingMessageChan chan types.Message
	sig                 chan os.Signal
}
