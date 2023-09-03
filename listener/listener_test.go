package listener

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	internalMocks "github.com/fatceken/solid-sqs-listener/listener/internal/mocks"
	"github.com/fatceken/solid-sqs-listener/listener/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_New(t *testing.T) {

	t.Run("Should return error when unable to get queue url", func(t *testing.T) {

		l, err := New(context.TODO(), &Settings{
			Region: "region",
		})

		assert.Nil(t, l)
		assert.Error(t, err)
	})
}

func Test_performConsume(t *testing.T) {

	t.Run("Should not return error when message is consumed successfully", func(t *testing.T) {

		//arrange
		msg := types.Message{}

		listener := &Listener{
			settings: &Settings{
				queueUrl:      aws.String("mockqueueUrl"),
				MaxRetryCount: 3,
			},
			context: context.TODO(),
		}

		mockConsumer := mocks.NewConsumer(t)
		mockConsumer.On("Consume", msg).Once().Return(nil)

		//act
		got := listener.performConsume(msg, mockConsumer)

		//assert
		assert.NoError(t, got)
	})

	t.Run("Should return error when message is consumed unsuccessfully", func(t *testing.T) {

		//arrange
		msg := types.Message{
			MessageId: aws.String("dummyMessageId"),
		}

		listener := &Listener{
			settings: &Settings{
				queueUrl:      aws.String("mockqueueUrl"),
				MaxRetryCount: 3,
			},
			context: context.TODO(),
		}

		mockConsumer := mocks.NewConsumer(t)
		mockConsumer.On("Consume", msg).Return(errors.New(mock.Anything)).Times(listener.settings.MaxRetryCount + 1)

		//act
		got := listener.performConsume(msg, mockConsumer)

		//assert
		assert.Error(t, got)
	})
}

func Test_sendAck(t *testing.T) {

	t.Run("Should not return error when message is acked successfully", func(t *testing.T) {

		//arrange
		msg := types.Message{
			ReceiptHandle: aws.String("dummyReceiptHandle"),
			MessageId:     aws.String("dummyMessageId"),
		}

		listener := &Listener{
			settings: &Settings{
				queueUrl: aws.String("mockqueueUrl"),
			},
			context: context.TODO(),
		}

		mockApiFace := internalMocks.NewSqsApiFace(t)
		mockApiFace.On("DeleteMessage", listener.context, &sqs.DeleteMessageInput{
			QueueUrl:      listener.settings.queueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		}).Return(&sqs.DeleteMessageOutput{}, nil)

		listener.client = mockApiFace

		//act
		got := listener.sendAck(msg)

		//assert
		assert.NoError(t, got)
	})

	t.Run("Should return error when message is acked unsuccessfully", func(t *testing.T) {

		//arrange
		msg := types.Message{
			ReceiptHandle: aws.String("dummyReceiptHandle"),
			MessageId:     aws.String("dummyMessageId"),
		}

		listener := &Listener{
			settings: &Settings{
				queueUrl: aws.String("mockqueueUrl"),
			},
			context: context.TODO(),
		}

		mockApiFace := internalMocks.NewSqsApiFace(t)
		mockApiFace.On("DeleteMessage", listener.context, &sqs.DeleteMessageInput{
			QueueUrl:      listener.settings.queueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		}).Return(&sqs.DeleteMessageOutput{}, errors.New(mock.Anything))

		listener.client = mockApiFace

		//act
		got := listener.sendAck(msg)

		//assert
		assert.Error(t, got)
	})
}

func Test_welcomeMessages(t *testing.T) {

	t.Run("should return no error when incoming message channel closed", func(t *testing.T) {

		msg := types.Message{
			ReceiptHandle: aws.String("dummyReceiptHandle"),
			MessageId:     aws.String("dummyMessageId"),
		}

		listener := &Listener{
			settings: &Settings{
				queueUrl:      aws.String("mockqueueUrl"),
				MaxRetryCount: 1,
			},
			context:             context.TODO(),
			incomingMessageChan: make(chan types.Message),
		}

		mockApiFace := &internalMocks.SqsApiFace{}
		mockApiFace.On("DeleteMessage", listener.context, &sqs.DeleteMessageInput{
			QueueUrl:      listener.settings.queueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		}).Return(&sqs.DeleteMessageOutput{}, nil)

		listener.client = mockApiFace

		mockConsumer := &mocks.Consumer{}
		mockConsumer.On("Consume", msg).Return(nil)

		close(listener.incomingMessageChan)

		got := listener.welcomeMessages(mockConsumer)
		assert.NoError(t, got)
	})
}

func Test_pollMessages(t *testing.T) {
	t.Run("should return error when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		listener := &Listener{
			context:             ctx,
			incomingMessageChan: make(chan types.Message),
			settings: &Settings{
				Delay: time.Second * 5,
			},
		}
		cancel()

		got := listener.pollMessages()

		assert.EqualError(t, got, context.Canceled.Error())
	})

	t.Run("should return error when unable to receive messages from queue", func(t *testing.T) {
		listener := &Listener{
			settings: &Settings{
				queueUrl:      aws.String("mockqueueUrl"),
				MaxRetryCount: 1,
				Delay:         time.Second * 3,
				WaitTime:      5,
			},
			context:             context.TODO(),
			incomingMessageChan: make(chan types.Message),
		}

		mockApiFace := &internalMocks.SqsApiFace{}
		mockApiFace.On("ReceiveMessage", listener.context, &sqs.ReceiveMessageInput{
			QueueUrl: listener.settings.queueUrl,
			AttributeNames: []types.QueueAttributeName{
				"SentTimestamp",
			},
			MaxNumberOfMessages: 1,
			MessageAttributeNames: []string{
				"All",
			},
			WaitTimeSeconds: int32(listener.settings.WaitTime),
		}).Return(&sqs.ReceiveMessageOutput{}, errors.New(mock.Anything))

		listener.client = mockApiFace

		got := listener.pollMessages()

		assert.EqualError(t, got, mock.Anything)
	})
}

func Test_Listen(t *testing.T) {

	t.Run("should return no error when os signal sent to system", func(t *testing.T) {

		msg := types.Message{
			ReceiptHandle: aws.String("dummyReceiptHandle"),
			MessageId:     aws.String("dummyMessageId"),
		}

		listener := &Listener{
			settings: &Settings{
				queueUrl:            aws.String("mockqueueUrl"),
				MaxRetryCount:       1,
				Delay:               time.Second * 3,
				WaitTime:            5,
				ShutDownGracePeriod: time.Second * 5,
			},
			context:             context.TODO(),
			incomingMessageChan: make(chan types.Message),
			sig:                 make(chan os.Signal, 1),
		}

		mockApiFace := &internalMocks.SqsApiFace{}
		mockApiFace.On("ReceiveMessage", listener.context, &sqs.ReceiveMessageInput{
			QueueUrl: listener.settings.queueUrl,
			AttributeNames: []types.QueueAttributeName{
				"SentTimestamp",
			},
			MaxNumberOfMessages: 1,
			MessageAttributeNames: []string{
				"All",
			},
			WaitTimeSeconds: int32(listener.settings.WaitTime),
		}).Return(&sqs.ReceiveMessageOutput{
			Messages: []types.Message{
				msg,
			},
		}, nil)

		mockApiFace.On("DeleteMessage", listener.context, &sqs.DeleteMessageInput{
			QueueUrl:      listener.settings.queueUrl,
			ReceiptHandle: msg.ReceiptHandle,
		}).Return(&sqs.DeleteMessageOutput{}, nil)

		listener.client = mockApiFace

		mockConsumer := mocks.NewConsumer(t)
		mockConsumer.On("Consume", msg).Return(nil)

		go func() {
			time.Sleep(time.Second * 3)
			listener.sig <- syscall.SIGTERM
		}()

		got := listener.Listen(mockConsumer)

		assert.NoError(t, got)

	})
}
