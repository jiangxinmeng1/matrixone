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
	"context"
	"encoding/csv"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type dumpTableFSOptions struct {
	output  string
	s3      string
	backend string
}

func newDumpTableFileService(ctx context.Context, opts dumpTableFSOptions) (fileservice.FileService, error) {
	output := strings.TrimSpace(opts.output)
	if output == "" {
		return nil, moerr.NewInvalidInputNoCtx("dump table output directory is empty")
	}
	s3Opts := strings.TrimSpace(opts.s3)
	if s3Opts == "" {
		return fileservice.GetForBackup(ctx, output)
	}
	service, err := makeDumpTableS3Service(opts.backend, s3Opts)
	if err != nil {
		return nil, err
	}
	return fileservice.GetForBackup(ctx, fileservice.JoinPath(service, output))
}

func makeDumpTableS3Service(backend string, s3Opts string) (string, error) {
	values := make(map[string]string)
	for _, item := range strings.Split(s3Opts, ",") {
		kv := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(kv) != 2 || strings.TrimSpace(kv[0]) == "" {
			return "", moerr.NewInvalidInputNoCtxf("invalid s3 option: %s", item)
		}
		values[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}

	required := []string{"bucket", "endpoint", "key-prefix", "key-id", "key-secret"}
	for _, key := range required {
		if values[key] == "" {
			return "", moerr.NewInvalidInputNoCtxf("missing s3 option: %s", key)
		}
	}

	isMinio := false
	switch {
	case backend == "", strings.EqualFold(backend, "S3"):
	case strings.EqualFold(backend, "MINIO"):
		isMinio = true
	default:
		return "", moerr.NewInvalidInputNoCtxf("invalid s3 backend: %s", backend)
	}
	region := values["region"]
	if region == "" {
		region = "us-east-1"
	}

	buf := new(strings.Builder)
	writer := csv.NewWriter(buf)
	err := writer.Write([]string{
		"s3-opts",
		"endpoint=" + values["endpoint"],
		"region=" + region,
		"key=" + values["key-id"],
		"secret=" + values["key-secret"],
		"bucket=" + values["bucket"],
		"prefix=" + values["key-prefix"],
		"is-minio=" + boolString(isMinio),
	})
	if err != nil {
		return "", err
	}
	writer.Flush()
	return buf.String(), writer.Error()
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func dumpTablePath(elem ...string) string {
	parts := make([]string, 0, len(elem))
	for _, part := range elem {
		if part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return path.Join(parts...)
}

func dumpTableEntryDir(name string, id uint64) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(name) + "_" + uint64String(id)
}

func uint64String(v uint64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}
