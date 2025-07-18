// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

type lockTableKeeper struct {
	serviceID                 string
	client                    Client
	stopper                   *stopper.Stopper
	keepLockTableBindInterval time.Duration
	keepRemoteLockInterval    time.Duration
	groupTables               *lockTableHolders
	service                   *service
}

// NewLockTableKeeper create a locktable keeper, an internal timer is started
// to send a keepalive request to the lockTableAllocator every interval, so this
// interval needs to be much smaller than the real lockTableAllocator's timeout.
func NewLockTableKeeper(
	serviceID string,
	client Client,
	keepLockTableBindInterval time.Duration,
	keepRemoteLockInterval time.Duration,
	groupTables *lockTableHolders,
	service *service,
) LockTableKeeper {
	s := &lockTableKeeper{
		serviceID:                 serviceID,
		client:                    client,
		groupTables:               groupTables,
		keepLockTableBindInterval: keepLockTableBindInterval,
		keepRemoteLockInterval:    keepRemoteLockInterval,
		service:                   service,
		stopper: stopper.NewStopper("lock-table-keeper",
			stopper.WithLogger(service.logger.RawLogger())),
	}
	if err := s.stopper.RunTask(s.keepLockTableBind); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.keepRemoteLock); err != nil {
		panic(err)
	}
	return s
}

func (k *lockTableKeeper) Close() error {
	k.stopper.Stop()
	return nil
}

func (k *lockTableKeeper) keepLockTableBind(ctx context.Context) {
	defer k.service.logger.InfoAction("keep lock table bind task")()

	timer := time.NewTimer(k.keepLockTableBindInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			k.doKeepLockTableBind(ctx)
			timer.Reset(k.keepLockTableBindInterval)
		}
	}
}

func (k *lockTableKeeper) keepRemoteLock(ctx context.Context) {
	defer k.service.logger.InfoAction("keep remote locks task")()

	timer := time.NewTimer(k.keepRemoteLockInterval)
	defer timer.Stop()

	services := make(map[string]pb.LockTable)
	var futures []*morpc.Future
	var binds []pb.LockTable
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			futures, binds = k.doKeepRemoteLock(
				ctx,
				futures,
				services,
				binds)
			timer.Reset(k.keepRemoteLockInterval)
		}
	}
}

func (k *lockTableKeeper) doKeepRemoteLock(
	ctx context.Context,
	futures []*morpc.Future,
	services map[string]pb.LockTable,
	binds []pb.LockTable) ([]*morpc.Future, []pb.LockTable) {
	for k := range services {
		delete(services, k)
	}
	binds = binds[:0]
	futures = futures[:0]

	k.groupTables.iter(func(_ uint64, v lockTable) bool {
		bind := v.getBind()
		if bind.ServiceID != k.serviceID {
			services[bind.ServiceID] = bind
		}
		return true
	})
	if len(services) == 0 {
		return futures[:0], binds[:0]
	}

	ctx, cancel := context.WithTimeoutCause(ctx, defaultRPCTimeout, moerr.CauseDoKeepRemoteLock)
	defer cancel()
	for _, bind := range services {
		req := acquireRequest()
		defer releaseRequest(req)

		req.Method = pb.Method_KeepRemoteLock
		req.LockTable = bind
		req.KeepRemoteLock.ServiceID = k.serviceID

		f, err := k.client.AsyncSend(ctx, req)
		if err == nil {
			futures = append(futures, f)
			binds = append(binds, bind)
			continue
		}
		err = moerr.AttachCause(ctx, err)
		logKeepRemoteLocksFailed(k.service.logger, bind, err)
		if !isRetryError(err) {
			k.groupTables.removeWithFilter(func(_ uint64, v lockTable) bool {
				return !v.getBind().Changed(bind)
			})
		}
	}

	for idx, f := range futures {
		v, err := f.Get()
		if err == nil {
			releaseResponse(v.(*pb.Response))
		} else {
			logKeepRemoteLocksFailed(k.service.logger, binds[idx], err)
		}
		f.Close()
		futures[idx] = nil // gc
	}
	return futures[:0], binds[:0]
}

func (k *lockTableKeeper) doKeepLockTableBind(ctx context.Context) {
	if k.service.isStatus(pb.Status_ServiceLockWaiting) &&
		k.service.activeTxnHolder.empty() {
		k.service.setStatus(pb.Status_ServiceUnLockSucc)
	}

	req := acquireRequest()
	defer releaseRequest(req)

	oldVersion := k.groupTables.getVersion()
	req.Method = pb.Method_KeepLockTableBind
	req.KeepLockTableBind.ServiceID = k.serviceID
	req.KeepLockTableBind.Status = k.service.getStatus()
	if !k.service.isStatus(pb.Status_ServiceLockEnable) {
		req.KeepLockTableBind.LockTables = k.service.topGroupTables()
		req.KeepLockTableBind.TxnIDs = k.service.activeTxnHolder.getAllTxnID()
	}

	ctx, cancel := context.WithTimeoutCause(ctx, k.keepLockTableBindInterval, moerr.CauseDoKeepLockTableBind)
	defer cancel()
	resp, err := k.client.Send(ctx, req)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logKeepBindFailed(k.service.logger, err)
		return
	}
	defer releaseResponse(resp)

	if resp.KeepLockTableBind.OK {
		switch resp.KeepLockTableBind.Status {
		case pb.Status_ServiceLockEnable:
			if !k.service.isStatus(pb.Status_ServiceLockEnable) {
				k.service.logger.Error("tn has abnormal lock service status",
					zap.String("serviceID", k.serviceID),
					zap.String("status", k.service.getStatus().String()))
			}
			return
		case pb.Status_ServiceLockWaiting:
			// maybe pb.Status_ServiceUnLockSucc
			if k.service.isStatus(pb.Status_ServiceLockEnable) {
				go k.service.checkCanMoveGroupTables()
			}
		default:
			k.service.setStatus(resp.KeepLockTableBind.Status)
		}
		if len(req.KeepLockTableBind.LockTables) > 0 {
			logBindsMove(k.service.logger, k.service.popGroupTables())
			logStatus(k.service.logger, k.service.getStatus())
		}
		return
	}

	n := 0
	k.groupTables.removeWithFilter(func(_ uint64, v lockTable) bool {
		newVersion := k.groupTables.getVersion()
		if oldVersion != newVersion {
			return false
		}
		bind := v.getBind()
		if bind.ServiceID == k.serviceID {
			n++
			return true
		}
		return false
	})

	if n > 0 {
		// Keep bind receiving an explicit failure means that all the binds of the local
		// lock table are invalid. We just need to remove it from the map, and the next
		// time we access it, we will automatically get the latest bind from allocate.
		logLocalBindsInvalid(k.service.logger)
	}
}
