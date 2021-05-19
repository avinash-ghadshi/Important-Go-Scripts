package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var wg sync.WaitGroup

func main() {
	fmt.Printf("Starting workers\n")
	count := 3
	inJobs := make(chan int)

	//Start Workers as routines
	for i := 0; i < count; i++ {
		go worker(i+1, inJobs)
		wg.Add(1)
	}

	//Start The job creation .... blocking call
	createJobs(inJobs)

	// Tell workers all jobs are done
	close(inJobs)

	// Wait for workers to finish
	wg.Wait()

	fmt.Printf("All done\n")

}

func createJobs(inJobs chan int) {
	// Read the file and call  ... Giving demo with a static array
	data := []int{11, 22, 33, 44, 55, 66, 77, 88, 99, 111, 222, 333, 444, 555, 666}
	for _, job1 := range data {
		inJobs <- job1
	}
}

// Each worker will execute the jobs
func worker(workerID int, inJobs chan int) {
	defer wg.Done()
	fmt.Printf("Starting worker %d\n", workerID)
	for jobData := range inJobs {
		fmt.Printf("%d) Worker doing job %d\n", workerID, jobData)
		randomSleep()
	}
	fmt.Printf("%d) Worker  Completed\n", workerID)

}

//sleep for 0 - 400 ms
func randomSleep() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(400) // n will be between 0 and 400
	fmt.Printf("Sleeping %d ms...\n", n)
	time.Sleep(time.Duration(n) * time.Millisecond)
}
