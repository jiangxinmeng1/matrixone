// Copyright 2026 Matrix Origin
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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func retryPause(attempt int) {
	interval := 100 * time.Millisecond
	for step := 0; step < attempt && interval < 4*time.Second; step++ {
		interval *= 2
	}
	time.Sleep(interval)
}

func runCycle(
	client logservice.Client, prefix string, count int,
	tryTimeout, retryWindow time.Duration, progressFile, stopFile string,
	pauseReadSeq int, readReadyFile, readReleaseFile string,
) error {
	var appendRetries, readRetries int
	var maxCycle time.Duration
	for seq := 0; count == 0 || seq < count; seq++ {
		if stopFile != "" {
			if _, err := os.Stat(stopFile); err == nil {
				fmt.Printf("summary cycles=%d append_retries=%d read_retries=%d max_cycle_ms=%.3f\n",
					seq, appendRetries, readRetries, float64(maxCycle.Microseconds())/1000)
				return nil
			}
		}

		payload := []byte(fmt.Sprintf("%s/%06d", prefix, seq))
		record := pb.NewUserLogRecord(100, len(payload))
		record.SetPayload(payload)
		cycleStarted := time.Now()
		var index uint64
		var appendAttempts, readAttempts int
		var appendElapsed, readElapsed time.Duration

		for attempt := 1; ; attempt++ {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), tryTimeout)
			lsn, err := client.Append(ctx, record)
			cancel()
			if err == nil {
				index, appendAttempts = lsn, attempt
				appendElapsed = time.Since(cycleStarted)
				break
			}
			appendRetries++
			fmt.Printf("retry time=%s op=append seq=%d attempt=%d elapsed_ms=%.3f err=%q\n",
				time.Now().Format(time.RFC3339Nano), seq, attempt,
				float64(time.Since(started).Microseconds())/1000, err.Error())
			if time.Since(cycleStarted) > retryWindow {
				return fmt.Errorf("append seq=%d failed after %d attempts and %s: %w", seq, attempt, time.Since(cycleStarted), err)
			}
			retryPause(attempt)
		}
		if seq == pauseReadSeq {
			if readReadyFile == "" || readReleaseFile == "" {
				return fmt.Errorf("paused Read requires both ready and release files")
			}
			if err := os.WriteFile(readReadyFile, []byte(fmt.Sprintf("seq=%d lsn=%d\n", seq, index)), 0o644); err != nil {
				return err
			}
			fmt.Printf("read_paused time=%s seq=%d lsn=%d\n", time.Now().Format(time.RFC3339Nano), seq, index)
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := os.Stat(readReleaseFile); err == nil {
					break
				} else if !os.IsNotExist(err) {
					return err
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("timed out waiting to release Read seq=%d", seq)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}

		readStarted := time.Now()
		for attempt := 1; ; attempt++ {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), tryTimeout)
			records, _, err := client.Read(ctx, index, 1<<20)
			cancel()
			if err == nil {
				found := false
				for _, got := range records {
					if got.Lsn != index {
						continue
					}
					if got.Type != pb.UserRecord || len(got.Data) < 12 || string(got.Data[12:]) != string(payload) {
						return fmt.Errorf("read mismatch seq=%d lsn=%d type=%v payload=%q", seq, index, got.Type, got.Data)
					}
					found = true
					break
				}
				if found {
					readAttempts = attempt
					readElapsed = time.Since(readStarted)
					break
				}
				err = fmt.Errorf("lsn %d not returned", index)
			}
			readRetries++
			fmt.Printf("retry time=%s op=read seq=%d lsn=%d attempt=%d elapsed_ms=%.3f err=%q\n",
				time.Now().Format(time.RFC3339Nano), seq, index, attempt,
				float64(time.Since(started).Microseconds())/1000, err.Error())
			if time.Since(readStarted) > retryWindow {
				return fmt.Errorf("read seq=%d lsn=%d failed after %d attempts and %s: %w", seq, index, attempt, time.Since(readStarted), err)
			}
			retryPause(attempt)
		}

		elapsed := time.Since(cycleStarted)
		if elapsed > maxCycle {
			maxCycle = elapsed
		}
		fmt.Printf("cycle time=%s seq=%d lsn=%d append_attempts=%d read_attempts=%d append_ms=%.3f read_ms=%.3f total_ms=%.3f\n",
			time.Now().Format(time.RFC3339Nano), seq, index, appendAttempts, readAttempts,
			float64(appendElapsed.Microseconds())/1000, float64(readElapsed.Microseconds())/1000,
			float64(elapsed.Microseconds())/1000)
		if progressFile != "" {
			temporary := progressFile + ".tmp"
			if err := os.WriteFile(temporary, []byte(fmt.Sprintf("%d %d\n", seq, index)), 0o644); err != nil {
				return err
			}
			if err := os.Rename(temporary, progressFile); err != nil {
				return err
			}
		}
	}
	fmt.Printf("summary cycles=%d append_retries=%d read_retries=%d max_cycle_ms=%.3f\n",
		count, appendRetries, readRetries, float64(maxCycle.Microseconds())/1000)
	return nil
}
