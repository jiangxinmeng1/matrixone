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

package live_dump

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

type commonOptions struct {
	endpoint string
	user     string
	password string
}

type tableInfo struct {
	accountID uint32
	dbID      uint64
	dbName    string
	tableID   uint64
	tableName string
	relKind   string
}

func PrepareCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "live-dump",
		Short: "Online object dump/apply tool",
		Long:  "Dump and apply MatrixOne table data from a live cluster using TN inspect object paths.",
	}
	cmd.AddCommand(listCommand())
	cmd.AddCommand(dumpCommand())
	cmd.AddCommand(applyCommand())
	return cmd
}

func addCommonFlags(cmd *cobra.Command, opts *commonOptions, endpointFlag string) {
	cmd.Flags().StringVar(&opts.endpoint, endpointFlag, "127.0.0.1:6001", "MatrixOne endpoint in host:port format")
	cmd.Flags().StringVar(&opts.user, "user", "dump", "MatrixOne user")
	cmd.Flags().StringVar(&opts.password, "password", "111", "MatrixOne password")
}

func openDB(opts commonOptions) (*sql.DB, error) {
	if opts.endpoint == "" {
		return nil, errors.New("endpoint is required")
	}
	host, port, err := net.SplitHostPort(opts.endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %q: %w", opts.endpoint, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?charset=utf8mb4&parseTime=true&multiStatements=false",
		opts.user, opts.password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func listCommand() *cobra.Command {
	var opts commonOptions
	var listType string
	var accountID uint32
	var databaseID uint64
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List live accounts, databases, or tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB(opts)
			if err != nil {
				return err
			}
			defer db.Close()
			switch strings.ToLower(listType) {
			case "", "tables":
				return printTables(cmd, db, accountID, databaseID)
			case "databases", "dbs":
				return printDatabases(cmd, db, accountID)
			case "accounts":
				return printAccounts(cmd, db)
			default:
				return fmt.Errorf("unsupported list type %q", listType)
			}
		},
	}
	addCommonFlags(cmd, &opts, "source")
	cmd.Flags().StringVar(&listType, "type", "tables", "list type: tables, databases, or accounts")
	cmd.Flags().Uint32Var(&accountID, "account-id", 0, "filter by account id")
	cmd.Flags().Uint64Var(&databaseID, "database-id", 0, "filter by database id")
	return cmd
}

func dumpCommand() *cobra.Command {
	var opts commonOptions
	var tableID, databaseID uint64
	var accountID uint32
	var cluster bool
	var output, outputDir string
	var jobs int
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump live table/database/account/cluster data",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB(opts)
			if err != nil {
				return err
			}
			defer db.Close()
			tables, err := resolveDumpTables(db, tableID, databaseID, accountID, cluster)
			if err != nil {
				return err
			}
			root := outputDir
			if root == "" {
				root = output
			}
			if root == "" {
				return errors.New("--output/-o or --output-dir is required")
			}
			_ = jobs // v1 runs serially; keep the flag for CLI compatibility.
			// Obtain a single snapshot timestamp for consistent cross-table dump.
			getTSSQL := fmt.Sprintf("select mo_ctl('dn', 'inspect', %s)", sqlString("get-ts"))
			resp, err := querySingleString(db, getTSSQL)
			if err != nil {
				return fmt.Errorf("get snapshot timestamp: %w", err)
			}
			snapshotTS, err := parseSnapshotTS(resp)
			if err != nil {
				return fmt.Errorf("parse snapshot timestamp: %w", err)
			}
			for _, tbl := range tables {
				dir := tableDumpDir(root, tbl)
				operation := fmt.Sprintf(
					"dump-table -d %d -t %d -o %s --snapshot-ts %s",
					tbl.dbID,
					tbl.tableID,
					shellArg(dir),
					snapshotTS,
				)
				sqlText := fmt.Sprintf("select mo_ctl('dn', 'inspect', %s)", sqlString(operation))
				resp, err := querySingleString(db, sqlText)
				if err != nil {
					return fmt.Errorf("dump %s.%s(%d): %w", tbl.dbName, tbl.tableName, tbl.tableID, err)
				}
				if err := copyDumpOutput(dir); err != nil {
					return fmt.Errorf("dump %s.%s(%d): copy output: %w", tbl.dbName, tbl.tableName, tbl.tableID, err)
				}
				cmd.Printf("Table %d %s.%s dumped to %s\n%s\n", tbl.tableID, tbl.dbName, tbl.tableName, dir, strings.TrimSpace(resp))
			}
			cmd.Printf("Dumped %d tables to %s\n", len(tables), root)
			return nil
		},
	}
	addCommonFlags(cmd, &opts, "source")
	cmd.Flags().Uint64Var(&tableID, "table-id", 0, "table id to dump")
	cmd.Flags().Uint64Var(&databaseID, "database-id", 0, "database id to dump")
	cmd.Flags().Uint32Var(&accountID, "account-id", 0, "account id to dump")
	cmd.Flags().BoolVar(&cluster, "cluster", false, "dump all visible supported tables")
	cmd.Flags().StringVarP(&output, "output", "o", "", "dump output root")
	cmd.Flags().StringVar(&outputDir, "output-dir", "", "dump output root for batch scopes")
	cmd.Flags().IntVar(&jobs, "jobs", 5, "number of concurrent table jobs; v1 executes serially")
	return cmd
}

func applyCommand() *cobra.Command {
	var opts commonOptions
	var from string
	var targetDatabase string
	var targetTable string
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a live object dump package",
		RunE: func(cmd *cobra.Command, args []string) error {
			if from == "" {
				return errors.New("--from is required")
			}
			if targetDatabase == "" {
				return errors.New("--target-database is required")
			}
			db, err := openDB(opts)
			if err != nil {
				return err
			}
			defer db.Close()

			// Resolve tables to apply. If --target-table is given, apply a single
			// table. Otherwise scan --from for table subdirectories and apply each
			// one with its original name.
			var tasks []applyTableTask

			tmpRoot := filepath.Join("mo-data", "tmp", DumpTableDir)
			if targetTable != "" {
				// Single table apply
				tasks = append(tasks, applyTableTask{dir: from, tableName: targetTable})
			} else {
				// Database-level apply: scan for table directories
				tasks, err = resolveApplyTables(from)
				if err != nil {
					return err
				}
				if len(tasks) == 0 {
					return fmt.Errorf("no table directories found under %s", from)
				}
			}

			for _, t := range tasks {
				// Copy dump files to tmpfs so the TN handler can read them.
				tmpDir := filepath.Join(tmpRoot, t.dir)
				if err := copyDir(t.dir, tmpDir); err != nil {
					return fmt.Errorf("copy to tmpfs %s: %w", t.dir, err)
				}

				// Allow empty table name → TN reads original name from dump.
				operation := fmt.Sprintf(
					"apply-table-data -d %s -t %s -o %s",
					shellArg(targetDatabase),
					shellArg(t.tableName),
					shellArg(t.dir),
				)
				sqlText := fmt.Sprintf("select mo_ctl('dn', 'inspect', %s)", sqlString(operation))
				resp, err := querySingleString(db, sqlText)
				if err != nil {
					return fmt.Errorf("apply %s: %w", t.dir, err)
				}
				cmd.Println(strings.TrimSpace(resp))
			}
			return nil
		},
	}
	addCommonFlags(cmd, &opts, "target")
	cmd.Flags().StringVar(&from, "from", "", "dump directory to apply")
	cmd.Flags().StringVar(&targetDatabase, "target-database", "", "target database name")
	cmd.Flags().StringVar(&targetTable, "target-table", "", "target table name (optional for database-level apply)")
	return cmd
}

func resolveDumpTables(db *sql.DB, tableID, databaseID uint64, accountID uint32, cluster bool) ([]tableInfo, error) {
	selected := 0
	if tableID != 0 {
		selected++
	}
	if databaseID != 0 {
		selected++
	}
	if accountID != 0 {
		selected++
	}
	if cluster {
		selected++
	}
	if selected != 1 {
		return nil, errors.New("exactly one of --table-id, --database-id, --account-id, or --cluster is required")
	}
	switch {
	case tableID != 0:
		return queryTables(db, "rel_id = "+strconv.FormatUint(tableID, 10))
	case databaseID != 0:
		return queryTables(db, "reldatabase_id = "+strconv.FormatUint(databaseID, 10))
	case accountID != 0:
		return queryTables(db, "account_id = "+strconv.FormatUint(uint64(accountID), 10))
	default:
		return queryTables(db, "1 = 1")
	}
}

func queryTables(db *sql.DB, where string) ([]tableInfo, error) {
	sqlText := fmt.Sprintf(`select account_id, reldatabase_id, reldatabase, rel_id, relname, relkind
from mo_catalog.mo_tables
where %s and relkind = 'r'
order by account_id, reldatabase_id, rel_id`, where)
	rows, err := db.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []tableInfo
	for rows.Next() {
		var tbl tableInfo
		if err = rows.Scan(&tbl.accountID, &tbl.dbID, &tbl.dbName, &tbl.tableID, &tbl.tableName, &tbl.relKind); err != nil {
			return nil, err
		}
		ret = append(ret, tbl)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, errors.New("no supported tables found")
	}
	return ret, nil
}

func printTables(cmd *cobra.Command, db *sql.DB, accountID uint32, databaseID uint64) error {
	conds := []string{"relkind = 'r'"}
	if accountID != 0 {
		conds = append(conds, "account_id = "+strconv.FormatUint(uint64(accountID), 10))
	}
	if databaseID != 0 {
		conds = append(conds, "reldatabase_id = "+strconv.FormatUint(databaseID, 10))
	}
	tables, err := queryTables(db, strings.Join(conds, " and "))
	if err != nil {
		return err
	}
	cmd.Println("account_id\tdatabase_id\tdatabase\ttable_id\ttable\trelkind")
	for _, tbl := range tables {
		cmd.Printf("%d\t%d\t%s\t%d\t%s\t%s\n", tbl.accountID, tbl.dbID, tbl.dbName, tbl.tableID, tbl.tableName, tbl.relKind)
	}
	return nil
}

func printDatabases(cmd *cobra.Command, db *sql.DB, accountID uint32) error {
	where := "1 = 1"
	if accountID != 0 {
		where = "account_id = " + strconv.FormatUint(uint64(accountID), 10)
	}
	rows, err := db.Query(fmt.Sprintf(`select account_id, dat_id, datname, dat_type
from mo_catalog.mo_database
where %s
order by account_id, dat_id`, where))
	if err != nil {
		return err
	}
	defer rows.Close()
	cmd.Println("account_id\tdatabase_id\tdatabase\ttype")
	for rows.Next() {
		var acc uint32
		var id uint64
		var name, typ string
		if err = rows.Scan(&acc, &id, &name, &typ); err != nil {
			return err
		}
		cmd.Printf("%d\t%d\t%s\t%s\n", acc, id, name, typ)
	}
	return rows.Err()
}

func printAccounts(cmd *cobra.Command, db *sql.DB) error {
	rows, err := db.Query(`select distinct account_id from mo_catalog.mo_database order by account_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	cmd.Println("account_id")
	for rows.Next() {
		var acc uint32
		if err = rows.Scan(&acc); err != nil {
			return err
		}
		cmd.Printf("%d\n", acc)
	}
	return rows.Err()
}

func querySingleString(db *sql.DB, sqlText string) (string, error) {
	var s sql.NullString
	if err := db.QueryRow(sqlText).Scan(&s); err != nil {
		return "", err
	}
	if !s.Valid {
		return "", nil
	}
	return s.String, nil
}

func tableDumpDir(root string, tbl tableInfo) string {
	if len(root) > 0 && root[len(root)-1] == '/' {
		root = strings.TrimRight(root, "/")
	}
	if root == "" {
		return fmt.Sprintf("tables/account_%d/db_%d/table_%d", tbl.accountID, tbl.dbID, tbl.tableID)
	}
	return fmt.Sprintf("%s/tables/account_%d/db_%d/table_%d", root, tbl.accountID, tbl.dbID, tbl.tableID)
}

func shellArg(s string) string {
	return strconv.Quote(s)
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// DumpTableDir matches the tmp fileservice app name used by dump-table and
// apply-table-data on the TN side.
const DumpTableDir = "dumpTable"

// resolveApplyTables scans the dump root directory for table subdirectories
// and returns a list of apply tasks. The directory structure is expected to be:
//
//	<root>/tables/account_<id>/db_<id>/table_<id>/
type applyTableTask struct {
	dir       string
	tableName string
}

// resolveApplyTables scans the dump root directory for table subdirectories
// and returns a list of apply tasks. The directory structure is expected to be:
//
//	<root>/tables/account_<id>/db_<id>/table_<id>/
func resolveApplyTables(root string) ([]applyTableTask, error) {
	var tasks []applyTableTask
	tableDirPattern := filepath.Join(root, "tables", "account_*", "db_*", "table_*")
	matches, err := filepath.Glob(tableDirPattern)
	if err != nil {
		return nil, err
	}
	for _, m := range matches {
		info, err := os.Stat(m)
		if err != nil || !info.IsDir() {
			continue
		}
		// Check that this directory contains the required dump files.
		schemaPath := filepath.Join(m, "schema")
		if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
			continue
		}
		// Pass empty table name → TN handler reads original name from dump.
		tasks = append(tasks, applyTableTask{dir: m, tableName: ""})
	}
	return tasks, nil
}

// copyDumpOutput copies dump output from the TN-side tmp fileservice
// (mo-data/tmp/dumpTable/) to the user-requested local directory.
// The dump-table inspect command writes to the dumpTable tmpfs app;
// this function makes the output visible at the path the user specified.
func copyDumpOutput(userPath string) error {
	tmpRoot := filepath.Join("mo-data", "tmp", "dumpTable")
	// Normalize userPath: strip leading ./ if present
	srcPath := filepath.Join(tmpRoot, userPath)
	// Ensure destination parent directories exist
	if err := os.MkdirAll(filepath.Dir(userPath), 0755); err != nil {
		return err
	}
	return copyDir(srcPath, userPath)
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}
		return copyFile(path, dstPath)
	})
}

// parseSnapshotTS extracts the timestamp string from a get-ts inspect command
// response. The InspectResp.ConsoleString() format is "\nmsg: <Message>\n\n<Payload>".
func parseSnapshotTS(resp string) (string, error) {
	s := strings.TrimSpace(resp)
	const prefix = "msg: "
	if !strings.HasPrefix(s, prefix) {
		return "", fmt.Errorf("unexpected get-ts response: %q", resp)
	}
	return strings.TrimSpace(strings.TrimPrefix(s, prefix)), nil
}

func copyFile(src, dst string) (err error) {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := d.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	_, err = io.Copy(d, s)
	return err
}
