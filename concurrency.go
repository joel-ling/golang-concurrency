/* Copyright 2021 Joel Ling

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. */

package main

import (
	"fmt"
	"math/rand"
	"sync"
	//"time"
)

type Job interface {
	// A Job maintains correspondence between an input and its output,
	// defines the operations for deriving an output from an input, and
	// has an identification number for recall.

	GetJobID() uint64

	GetInput() int

	GetOutput() int

	Do()
}

type SpecificJob struct {
	// A SpecificJob is a particular implementation of the Job interface.

	jobID  uint64
	input  int
	output int
}

func NewSpecificJob(input int) (job *SpecificJob) {
	// Create and return a SpecificJob given an input
	// without calculating its output, and
	// assign to it a random identification number.

	// The theoretical probability of collision of the
	// random identification number is 1 in 18.4 quintillion.
	// For scale, the age of the universe is estimated at
	// 0.4 quintillion seconds (about 14 billion years).

	job = &SpecificJob{
		jobID: rand.Uint64(),
		input: input,
	}

	return
}

func (j *SpecificJob) GetJobID() uint64 {
	return j.jobID
}

func (j *SpecificJob) GetInput() int {
	return j.input
}

func (j *SpecificJob) GetOutput() int {
	return j.output
}

func (j *SpecificJob) Do() {
	// Calculate and save the output.

	j.output = -1 * j.input // trivial example

	//time.Sleep(time.Second) // (optional) simulate time-consuming process
}

type Cluster struct {
	// A Cluster represents a group of worker goroutines that
	// execute jobs concurrently.
	// It provides methods for sending jobs to those goroutines and
	// receiving completed jobs from them.

	jobsToDo chan Job
	jobsDone map[uint64]Job
	mapMutex *sync.Mutex
}

func NewCluster(nWorkers uint) (cluster *Cluster) {
	// Create and return a Cluster and
	// launch a given number of worker goroutines.

	var (
		i uint
	)

	cluster = &Cluster{
		jobsToDo: make(chan Job),
		jobsDone: make(map[uint64]Job),
		mapMutex: new(sync.Mutex),
	}

	for i = 0; i < nWorkers; i++ {
		go cluster.doJobs()
	}

	return
}

func (c Cluster) doJobs() {
	// The inner workings of a worker goroutine.
	// Execute jobs placed in the cluster's to-do queue (a channel) and
	// index completed jobs (in a map) for quick, arbitrary retrieval.

	var (
		job Job
	)

	for job = range c.jobsToDo {
		job.Do()

		c.mapMutex.Lock() // because maps are not safe for concurrent access

		c.jobsDone[job.GetJobID()] = job

		c.mapMutex.Unlock()
	}
}

func (c Cluster) ShutDown() {
	// Signal the worker goroutines to exit their for loops and return by
	// closing the cluster's to-do channel,
	// on which the goroutines are listening.

	close(c.jobsToDo)
}

func (c Cluster) Collect(job Job) {
	// Place a job in the to-do queue
	// to be picked up by any idle worker goroutine in the cluster.

	c.jobsToDo <- job
}

func (c Cluster) Deliver(jobID uint64) (job Job) {
	// Retrieve a job from an indexed store of completed jobs
	// given its identification number,
	// return the job and free the slot it occupied.

	var (
		exists bool
	)

	for {
		c.mapMutex.Lock()

		job, exists = c.jobsDone[jobID]

		c.mapMutex.Unlock()

		if exists {
			c.mapMutex.Lock()

			delete(c.jobsDone, jobID)

			c.mapMutex.Unlock()

			break
		}
	}

	return
}

type Batch struct {
	// A Batch represents an ordered group of inputs
	// that should be processed concurrently
	// while preserving the same order in the outputs.
	// It assigns a cluster of worker goroutines to process those inputs.

	// While a Batch is part of a WaitGroup in this particular implementation,
	// it does not have to be,
	// e.g. when it conveys inputs contained in an API request.

	Input     []int
	Output    []int
	Cluster   *Cluster
	WaitGroup *sync.WaitGroup
}

func (b *Batch) ExecuteSpecificProcess() {
	// Make a SpecificJob out of every element in the input,
	// send these jobs to the assigned cluster for execution,
	// wait for the jobs to be completed and collate them.

	var (
		i      int
		job    Job
		jobIDs []uint64
	)

	defer b.WaitGroup.Done()

	jobIDs = make([]uint64,
		len(b.Input),
	)

	for i = 0; i < len(b.Input); i++ {
		job = NewSpecificJob(b.Input[i])

		b.Cluster.Collect(job)

		jobIDs[i] = job.GetJobID()
		// save job receipt to collect job when it is done
	}

	b.Output = make([]int,
		len(b.Input),
	)

	for i = 0; i < len(b.Input); i++ {
		job = b.Cluster.Deliver(jobIDs[i])

		b.Output[i] = job.GetOutput()
	}
}

func main() {
	// Make 10 batches of 10 input elements, and
	// assign a cluster of 100 goroutines to work on them concurrently.
	// Shut down the cluster when the work is done, then
	// print the inputs and corresponding outputs in every batch
	// to standard output for inspection.

	const (
		batchSize = 10
		nBatches  = 10
		nWorkers  = 100
	)

	var (
		batch     *Batch
		batches   []*Batch
		cluster   *Cluster
		i         int
		j         int
		waitGroup *sync.WaitGroup
	)

	cluster = NewCluster(nWorkers)

	waitGroup = new(sync.WaitGroup)

	batches = make([]*Batch, nBatches)

	for i = 0; i < nBatches; i++ {
		batch = &Batch{
			Input:     make([]int, batchSize),
			Cluster:   cluster,
			WaitGroup: waitGroup,
		}

		for j = 0; j < batchSize; j++ {
			batch.Input[j] = i*batchSize + j
		}

		waitGroup.Add(1)

		go batch.ExecuteSpecificProcess()

		batches[i] = batch
	}

	waitGroup.Wait()

	cluster.ShutDown()

	for i = 0; i < nBatches; i++ {
		fmt.Printf("%#v -> %#v\n", batches[i].Input, batches[i].Output)
	}
}
