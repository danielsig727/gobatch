package batch

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

type testTask struct {
	processed []([]int)
	procLock  sync.Mutex
}

func (t *testTask) Init() {
	t.processed = []([]int){}
}

func (t *testTask) Process(batch *BatchData) *BatchResult {

	batchSize := len(batch.Data)
	data := make([]int, batchSize)
	result := make([]Result, batchSize)
	for i, v := range batch.Data {
		data[i] = *(v.(*int))
		r := data[i] + 1
		result[i] = &r
	}
	fmt.Printf("got batch data: %v\n", data)

	// t.procLock.Lock()
	// t.processed = append(t.processed, data)
	// t.procLock.Unlock()

	return &BatchResult{
		Data: result,
	}
}

func TestTestTask(t *testing.T) {
	var task Task = &testTask{}
	task.Init()

	nData := 20
	// data := make([]int, nData)
	batch := &BatchData{
		Data: make([]Data, nData),
	}
	for i := 0; i < nData; i++ {
		// data[i] = i
		d := i
		batch.Data[i] = &d
	}
	r := task.Process(batch)
	for i, v := range r.Data {
		fmt.Println(i, *(v.(*int)))
	}
}

func TestBatchProcessor(t *testing.T) {
	var task Task = &testTask{}
	task.Init()

	bp, _ := NewBatchProcessor(task, 10, time.Second)
	bp.Start()
	defer bp.Stop()

	var (
		maxWorkers = 100
		sem        = semaphore.NewWeighted(int64(maxWorkers))
		nData      = 100000
	)

	// var wg sync.WaitGroup
	ctx := context.TODO()

	for i := 0; i < nData; i++ {
		// wg.Add(1)
		// fmt.Println("sending", i)
		err := sem.Acquire(ctx, 1)
		assert.NoError(t, err)

		go func(ctx context.Context, idx int) {
			// defer wg.Done()
			defer sem.Release(1)

			currCtx, canc := context.WithCancel(ctx)
			time_start := time.Now()
			for i := 0; i < 20; i++ {
				<-time.After(time.Millisecond)
			}
			time_submit := time.Now()
			rChan, err := bp.Submit(currCtx, &idx)
			assert.NoError(t, err)
			select {
			case result := <-rChan:
				latency := time.Now().Sub(time_start)
				fmt.Printf("[%d] got %v, latency = %v, waiting = %v\n", idx, *(result.(*int)),
					latency, time_submit.Sub(time_start))
				canc()
				return
			case <-time.After(5 * time.Second):
				fmt.Printf("[%d] timeout\n", idx)
				canc()
			}
		}(ctx, i)

		// time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("waiting")
	// wg.Wait()
	if err := sem.Acquire(ctx, int64(maxWorkers)); err != nil {
		log.Printf("Failed to acquire semaphore: %v", err)
	}
	// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
}
