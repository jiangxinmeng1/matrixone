// Send one TN heartbeat to an isolated LogService cluster and inspect expiry.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func main() {
	addresses := flag.String("addresses", "", "isolated three node RPC addresses")
	mode := flag.String("mode", "state", "send or state")
	replayed := flag.Uint64("replayed-lsn", 1, "last replayed physical LSN")
	flag.Parse()
	if *addresses != "198.18.51.10:58801,198.18.51.11:58811,198.18.51.12:58821" ||
		(*mode != "send" && *mode != "state") {
		panic("use only the isolated expiry test cluster")
	}
	const sid = "tn-expiry-snapshot-probe"
	const uuid = "9fb33fa1-5ae9-43fb-876c-73b72b319129"
	runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
	defer runtime.CompareAndDeleteServiceBasedRuntime(sid, runtime.DefaultRuntime())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := logservice.NewTNHAKeeperClient(ctx, sid, logservice.HAKeeperClientConfig{ServiceAddresses: strings.Split(*addresses, ",")})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if *mode == "send" {
		callCtx, callCancel := context.WithTimeout(context.Background(), 15*time.Second)
		batch, err := client.SendTNHeartbeat(callCtx, pb.TNStoreHeartbeat{
			UUID: uuid, ServiceAddress: "198.18.51.1:59999", ReplayedLsn: *replayed,
			Shards: []pb.TNShardInfo{{ShardID: 1, ReplicaID: 1}},
		})
		callCancel()
		if err != nil {
			panic(err)
		}
		fmt.Printf("heartbeat_sent replayed_lsn=%d commands=%d\n", *replayed, len(batch.Commands))
	}
	callCtx, callCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer callCancel()
	state, err := client.GetClusterState(callCtx)
	if err != nil {
		panic(err)
	}
	info, ok := state.TNState.Stores[uuid]
	fmt.Printf("checker_state tick=%d tn_present=%v tn_tick=%d tn_replayed_lsn=%d\n", state.Tick, ok, info.Tick, info.ReplayedLsn)
	details, err := client.GetClusterDetails(callCtx)
	if err != nil {
		panic(err)
	}
	found := false
	for _, store := range details.TNStores {
		if store.UUID == uuid {
			fmt.Printf("cluster_details tn_state=%s tn_tick=%d\n", store.State.String(), store.Tick)
			found = true
		}
	}
	if !ok || !found {
		fmt.Fprintln(os.Stderr, "test TN missing from HAKeeper state")
		os.Exit(2)
	}
}
