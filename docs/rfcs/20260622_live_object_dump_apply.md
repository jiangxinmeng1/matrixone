- Status: draft
- Start Date: 2026-06-22
- Authors:
- Implementation PR:
- Issue for this RFC:

# Live Object Dump and Apply

# Summary

This RFC proposes an online object-level dump/apply feature for MatrixOne.
Unlike the existing checkpoint CSV dump workflow, this feature runs while the
source cluster is alive, dumps table data as MatrixOne object data, and applies
the dump package into a target cluster by writing new objects.

The lowest-level implementation unit is a table. User-facing commands support
table, database, account, and cluster scopes. Database/account/cluster scopes
are implemented as orchestration over multiple table-level dump/apply tasks.

# Motivation

The checkpoint CSV dump tool is useful for offline inspection and logical
recovery, but it has different tradeoffs:

- It reads checkpoint files instead of a live cluster.
- It exports CSV and optionally generates LOAD DATA scripts.
- Recovery goes through SQL and LOAD DATA rather than object ingestion.

The proposed feature targets cases where we want to copy or recover data from a
running cluster while preserving MatrixOne's object-oriented storage path:

- Dump table data from an online source cluster.
- Avoid SQL/CSV conversion overhead.
- Recover table/database/account/cluster data by applying object data.
- Reuse the existing TAE object read/write and fileservice infrastructure.

# Technical Design

## User-facing command model

The CLI should follow the style used by the checkpoint dump tool:

```bash
./mo-tool live-dump list ...
./mo-tool live-dump dump ...
./mo-tool live-dump apply ...
./mo-tool live-dump info ...
```

The scope selection flags should also match the checkpoint tool style:

- `--table-id`
- `--database-id`
- `--account-id`
- `--cluster`

The command reads from a live MatrixOne instance through a CN/MySQL endpoint and
dispatches the real table-level work to TN-side inspect/RPC handlers. The
current inspect prototype already contains the relevant table-level shape:

```sql
select mo_ctl('dn', 'inspect', 'dump-table -d <db_id> -t <table_id>');
select mo_ctl('dn', 'inspect', 'apply-table-data -d <target_db> -t <target_table> -o <dump_dir>');
```

The production command should hide internal IDs where possible by providing a
`list` command and by accepting scoped IDs consistently.

## Dump package layout

Dump output is a structured object package, not SQL or CSV:

```text
<dump-root>/
  manifest.json
  tables/
    account_<account_id>/
      db_<database_id>/
        table_<table_id>/
          table_manifest.json
          schema
          table
          object_list
          objects/
            <dump-object-file>
            ...
```

`manifest.json` records:

- dump format version
- dump id
- source cluster information
- selected scope
- dump start/end time
- table list
- per-table snapshot timestamp
- dump output fileservice information when available

Each `table_manifest.json` records:

- account id
- database id/name
- table id/name
- relation kind
- snapshot timestamp
- row/object counters
- source schema checksum
- object list file

The existing `schema`, `table`, and `object_list` object files can reuse the
prototype structure used by `dump-table`.

## Table dump flow

For each table:

1. Start a read transaction on TN and use its start timestamp as the dump
   snapshot timestamp.
2. Write table schema metadata to `schema`.
3. Write table entry metadata to `table`.
4. Traverse tombstone object entries and collect visible tombstone object
   entries. Do not materialize all tombstone row ids in memory.
5. Traverse data object entries.
6. Skip data objects that are not visible or relevant at the snapshot timestamp.
7. For each remaining data object:
   - Scan the data object with user columns, source row id, and commit timestamp.
   - Filter rows by applying tombstones per source block. Persisted tombstones
     use the same block-level delete-mask path as the CN reader
     (`GetTombstonesByBlockId`); appendable tombstones are scanned one object at
     a time.
   - Skip the object if all rows are deleted after filtering.
   - Record object metadata in `object_list`.
   - Write a dump object file that contains only live data rows.
8. Write per-table counters and checksums into `table_manifest.json`.

The object list should include at least:

- `object_type`: data
- `object_stats`: serialized source object stats
- `create_ts`
- `delete_ts`
- `is_persisted`

Additional fields such as row count, object size, and checksum are recommended
for diagnostics and validation.

## Apply flow

Apply reads a dump package and writes data into the target cluster. It must not
preserve source object names.

For each table:

1. Read table manifest, `schema`, `table`, and `object_list`.
2. Create the target database. Fail if a database with the target name already
   exists.
3. Create the target table from dumped schema metadata.
4. Fail if a table with the target name already exists.
5. Iterate `object_list`.
6. Load data objects, which already contain only live rows from the dump
   snapshot.
7. Write the rows through the target object writer.
8. Let the target cluster allocate new object names/object ids.
9. Register the new object stats in the target transaction.
10. Commit the table transaction.

This differs from the inspect prototype where persisted source object files may
be copied by original name. The production apply path must rewrite objects so
the target object namespace remains independent from the source namespace.

## Tombstone handling

Dump uses source tombstones only while building the dump package. Tombstone
objects are not written into the dump package, and `object_list` contains data
objects only.

The resulting semantics are:

- Source tombstones are used during dump to remove invisible rows from data
  objects.
- Dumped data objects contain only visible rows.
- Target data objects contain only visible rows.
- Target object names are newly generated.
- No source tombstone object is registered in the target table.

This is the key behavioral difference from low-level debug object apply.

## Scope orchestration

The table-level implementation is the only data path. Wider scopes expand to
table tasks:

- Table scope: one table task.
- Database scope: enumerate all supported user tables in the database.
- Account scope: enumerate all supported databases and tables in the account.
- Cluster scope: enumerate all supported accounts, databases, and tables.

Views and unsupported relation kinds are listed in manifests but skipped by
data dump unless a later version implements metadata-only restoration for them.

The default consistency model is per-table snapshot consistency. A future
strict mode can acquire or pass one shared snapshot timestamp for all tables in
a wider scope.

## Fileservice and S3

The CLI should support the same input/output storage option style as the
checkpoint tool:

- local output path
- `--out-fs-config` and `--out-fs-name`
- `--out-s3` and `--out-backend`
- apply input through local path, `--fs-config`, or `--s3`

The dump package is ordinary fileservice data and can be moved between clusters
as long as the target apply command can read it.

## Safety controls

Apply should work on a normally started cluster. It must not depend on debug
mode or a debug-only `EnableApplyTableData` gate.

Default safety behavior:

- Do not overwrite an existing target table.
- Roll back the current table on any apply error.
- Stop a batch apply after the first failed table unless `--continue-on-error`
  is explicitly set.
- Write a batch report listing succeeded, failed, and skipped tables.

# Drawbacks

- It is more tightly coupled to MatrixOne object internals than CSV dump.
- Dump packages are not portable to other databases.
- Rewriting objects during apply requires more implementation work than copying
  files by original name.
- Database/account/cluster consistency is per-table by default unless a shared
  snapshot mode is implemented.

# Rationale / Alternatives

## Continue using checkpoint CSV dump

This remains useful for offline inspection and SQL-level recovery, but it does
not satisfy online object dump/apply requirements.

## Copy source object files by original name

This is simpler and close to the current debug prototype, but it risks object
namespace collisions and carries source tombstone state into the target. The
proposed design rewrites objects and only writes visible data.

## Implement database/account as separate storage primitives

The table is already the natural TAE unit for schema, object metadata, and
transactional apply. Wider scopes should be orchestration over table tasks.

# Unresolved Questions

- Whether v1 needs a strict shared snapshot mode for database/account/cluster
  dump.
- Whether views should be restored as metadata-only objects in v1.
- Whether target table overwrite should support an explicit `--replace` mode.
- The final command namespace: `mo-tool live-dump` vs extending `mo-tool ckp`
  with an online object mode.
