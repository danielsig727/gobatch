package batch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	SubmissionTimeout error = fmt.Errorf("Submission timeout")
	ContextCancelled  error = fmt.Errorf("Context cancelled")
)

type Data interface{}
type Result interface{}

type BatchData struct {
	Data []Data
}

type BatchResult struct {
	Data []Result
}

type Task interface {
	Init()
	Process(*BatchData) *BatchResult
}

type BatchProcessor interface {
	Start()
	Stop()
	Submit(context.Context, Data) (<-chan Result, error)
}

type submission struct {
	ctx        context.Context
	data       Data
	resultChan chan Result
}

type batchProcessorImpl struct {
	started      bool
	stopped      bool
	maxBatchSize int
	task         Task
	timeout      time.Duration

	submissionChan chan submission
	cancelChan     chan bool
	wg             sync.WaitGroup
}

func NewBatchProcessor(task Task, maxBatchSize int, timeout time.Duration) (BatchProcessor, error) {
	bp := &batchProcessorImpl{
		maxBatchSize:   maxBatchSize,
		task:           task,
		timeout:        timeout,
		submissionChan: make(chan submission),
		cancelChan:     make(chan bool),
	}
	return bp, nil
}

func processBatch(task Task, subms []submission) {
	batch := &BatchData{
		Data: make([]Data, len(subms)),
	}
	for i, s := range subms {
		batch.Data[i] = s.data
	}
	results := task.Process(batch)
	for i, r := range results.Data {
		subms[i].resultChan <- r
	}
}

func (bp *batchProcessorImpl) Start() {
	if bp.started {
		return
	}
	bp.started = true

	bp.wg.Add(1)
	go func() {
		defer bp.wg.Done()

		var buffer []submission
		var timeoutSubmitChan chan (bool)
	batchLoop:
		for {
			// fmt.Println("preparing for a new batch")
			buffer = make([]submission, 1)
			timeoutSubmitChan = make(chan bool)

			// get the first one
			select {
			case subm, ok := <-bp.submissionChan:
				// fmt.Println("got first of batch")
				if !ok {
					return
				}
				buffer[0] = subm
				bp.wg.Add(1)
				go func(timeoutSubmitChan chan bool) {
					defer bp.wg.Done()
					select {
					case <-time.After(bp.timeout):
						select {
						case timeoutSubmitChan <- true:
						case <-timeoutSubmitChan:
						default:
							panic(123)
						}
					case <-timeoutSubmitChan:
						return
					}
				}(timeoutSubmitChan)
			case <-bp.cancelChan:
				return
			}

			// get the rest of the batch or process submission
		batchCollectionLoop:
			for {
				select {
				case subm, ok := <-bp.submissionChan:
					// fmt.Println("got rest of batch - current length:")
					if !ok {
						return
					}
					buffer = append(buffer, subm)
					if len(buffer) >= bp.maxBatchSize {
						close(timeoutSubmitChan) // <- true
						break batchCollectionLoop
					}

				case _, ok := <-timeoutSubmitChan:
					if !ok {
						return
					}
					break batchCollectionLoop

				case <-bp.cancelChan:
					return
				}
			}

			// submit the batch
			bp.wg.Add(1)
			go func(subms []submission) {
				defer bp.wg.Done()
				processBatch(bp.task, subms)
			}(buffer)
			continue batchLoop
		}
	}()
}

func (bp *batchProcessorImpl) Stop() {
	if !bp.started || bp.stopped {
		return
	}
	bp.stopped = true
	close(bp.cancelChan)
	bp.wg.Wait()
}

func (bp *batchProcessorImpl) Submit(ctx context.Context, d Data) (<-chan Result, error) {
	subm := submission{
		ctx:        ctx,
		data:       d,
		resultChan: make(chan Result),
	}
	select {
	case <-ctx.Done():
		return nil, ContextCancelled
	case bp.submissionChan <- subm:
		return subm.resultChan, nil
	case <-time.After(1 * time.Second):
		return nil, SubmissionTimeout
	}
}
