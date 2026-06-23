// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/spf13/cobra"
)

type GetTSArg struct {
	inspectContext *inspectContext
}

func (c *GetTSArg) PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-ts",
		Short: "Get current snapshot timestamp",
		Run:   RunFactory(c),
	}
	return cmd
}

func (c *GetTSArg) FromCommand(cmd *cobra.Command) (err error) {
	if cmd.Flag("ictx") != nil {
		c.inspectContext = cmd.Flag("ictx").Value.(*inspectContext)
	} else {
		return moerr.NewInternalErrorNoCtx("inspect context not found")
	}
	return nil
}

func (c *GetTSArg) String() string {
	return "get-ts"
}

func (c *GetTSArg) Run() (err error) {
	ts := c.inspectContext.db.TxnMgr.Now()
	fmt.Fprint(c.inspectContext.out, ts.ToString())
	return nil
}
