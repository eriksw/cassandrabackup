// Copyright 2020 RetailNext, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoint

import "sync"

type Barrier struct {
	setupDone bool
	finished  bool
	lock      sync.Mutex

	cancelSent bool
	cancel     chan struct{}
	add        chan int

	abortOutput   chan struct{}
	proceedOutput chan struct{}
}

func (b *Barrier) Add(n int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.setupOnce()
	if b.finished {
		return
	}
	b.add <- n
}

func (b *Barrier) Done() {
	b.Add(-1)
}

func (b *Barrier) Abort() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.setupOnce()
	if !b.finished && !b.cancelSent {
		b.cancelSent = true
		close(b.cancel)
	}
}

func (b *Barrier) Wait() (abort, proceed <-chan struct{}) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.setupOnce()
	return b.abortOutput, b.proceedOutput
}

func (b *Barrier) setupOnce() {
	if b.setupDone {
		return
	}
	b.add = make(chan int)
	b.cancel = make(chan struct{})
	b.abortOutput = make(chan struct{})
	b.proceedOutput = make(chan struct{})
	go b.run()
	b.setupDone = true
}

func (b *Barrier) run() {
	var n int
DONE:
	for {
		select {
		case <-b.cancel:
			b.lock.Lock()
			break DONE
		case chg := <-b.add:
			n += chg
			if n <= 0 {
				b.lock.Lock()
				break DONE
			}
		}
	}

	// If we've broken the for loop, we have the lock.
	if b.cancelSent {
		close(b.abortOutput)
	} else {
		if n > 0 {
			panic("impossible?")
		}
		close(b.proceedOutput)
	}
	close(b.add)
	b.finished = true
	b.lock.Unlock()
}
