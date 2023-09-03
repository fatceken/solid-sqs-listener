# solid-sqs-listener

No need to poll the sqs queues any more. Just configure the settings and **solid-sqs-listener** does it for you. The only thing you need to do is implementing Consumer interface according to your needs. Send SIGTERM (ctrl-z or control-z) to shutdown listener gracefully. 



## Getting Started



### Prerequisites

- **default** aws profile must be configured with necessary permissions.

### Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/fatceken/solid-sqs-listener/listener"
)

func main() {

	ctx := context.TODO()

	l, err := listener.New(ctx, &listener.Settings{
		QueueName:           "MyFirstQueue", //the name of the sqs queue
		WaitTime:            20, // Wait time in secs to collect messages 
		Region:              "us-east-1", //queue aws region
		Delay:               time.Second * 5, // wait time before next poll request
		MaxRetryCount:       3, // If first consume attempt fails, retry MaxRetryCount times
		ShutDownGracePeriod: time.Second * 10, // wait before shutdown in order to consume already taken messages and not poll for new ones
	})

	if err != nil {
		fmt.Println(err)
	}
	l.Listen(&Worker{})

	//send SIGTERM to shutdown
}

type Worker struct {
}

func (w *Worker) Consume(msg types.Message) error {
	fmt.Println("This is the message got from the queue ", *msg.Body)
	return nil
	//if returned error is not nil, this method will be retried MaxRetryCount times
}
```


## Contributing

Any PRs are welcomed ! Plesae do not hesitate to contribute