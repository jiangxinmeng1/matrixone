
# MatrixOne WAL设计解析

为了保证持久性，事务要在提交前持久化。TAE中最新的数据先用WAL(Write Ahead Log)持久化到一个Log Backend中。后台会定期发起事务，转存这些改动到另一个Storage上，然后通知Log Backend销毁掉旧的WAL。

## 物理日志
MatrixOne的WAL是物理日志，记录每行更新发生的位置，而不是sql，这样，每次回放出来，底层的数据都是一样的。

## Log Backend
   Log Backend 需要实现这些接口
  1. Append，提交事务时Append WAL
   ```
    Append(entry) (Lsn, error)
   ```
  1. Read，重启时批量读取WAL
   ```
	Read(Lsn, maxSize) (entry, Lsn, error)
   ```
  2. Truncate，做checkpoint时销毁旧的WAL
   ```
	Truncate(lsn Lsn) error
   ```

  目前有两种Log Backend。一种基于文件系统。另一个是我们自研的高可靠低延迟的Log Service。

## Commit Pipeline

提交之前DN要把TAE先把事务的改动更新到内存，。检查冲突，然后为确认不会回滚的事务写WAL日志，确认日志持久化后，更新内存中的状态，标志成已提交。
![](./image/wal_8.jpg)

TAE中事务在进入提交队列前更新Memtable和Catalog，在提交pipeline中批量写WAL，然后快速地标记状态使更改可见完成提交。例如，有多个事务txn1，txn2，txn3，txn4一起来提交，

## Group Commit

  持久化WAL涉及到IO，非常耗时，经常是提交耗时的瓶颈。为了节省时间，TAE中批量向Log Backend中写入WAL。比如，在文件系统中fsync耗时很久。如果每条WAL都fsync，会耗费大量时间。基于文件系统的Log Backend中，多个WAL写完后统一只做一次fsync，这些WAL刷盘的时间成本之和近似一条WAL刷盘的时间。
  
![](./image/wal_9.jpg)

Log Service中支持并发写入，各条WAL刷盘的时间可以重合，这也能缩短写WAL的总时间，提高了提交的并发。

## 处理Log Backend的乱序LSN
TAE向Log Backend并发写入WAL时，写入成功的顺序和发出请求的顺序不一致，导致Log Backend中产生LSN是乱序的。提交事务和销毁日志的时候要处理这些乱序LSN。

提交时，更改变得可见的顺序应该和事务提交的顺序一致，

## Checkpoint

DN会选一个合适的时间戳作为checkpoint，然后扫描时间戳之前的修改。

![](./image/wal_10.jpg)

DN先转存DML修改。DML更改存在Memtable中的各个block中。Logtail Mgr中记录着每个事务改动了哪些block。DN在Logtail Mgr上扫描[t0,t1]之间的事务，发起后台事务把这些block转存到S3上，在元数据中记录地址。这样，所有t1前提交的DML更改都能通过元数据中的地址查到。为了及时做checkpoint，控制WAL的大小，哪怕区间中block只改动了一行，也需要转存。

![](./image/wal_11.jpg)

然后扫描Catalog，收集[t0,t1]之间的DDL和元数据更改，转存到S3上。Logtail Mgr中存了每个事务对应的WAL LSN。根据时间戳，找到t1前最后一个事务，然后通知Log Backend清理这个LSN之前的所有日志。

## WAL格式

每个写事务对应一条WAL日志，由LSN，Transaction Context和多个Command组成。

```
+---------------------------------------------------------+
|                  Transaction Entry                      |
+-----+---------------------+-----------+-----------+-   -+
| LSN | Transaction Context | Command-1 | Command-2 | ... |
+-----+---------------------+-----------+-----------+-   -+
```

### LSN 
每条WAL对应一个LSN。LSN连续递增，在做checkpoint时用来删除WAL。

### Transaction Context

   Transaction Context里记录了事务的信息。
   ```
   +---------------------------+
   |   Transaction Context     |
   +---------+----------+------+
   | StartTS | CommitTS | Memo |
   +---------+----------+------+
   ```
   1. StartTS和CommitTS分别是事务开始和结束的时间戳。
   2. Memo记录事务更改了哪些地方的数据。重启的时候，会把这些信息恢复到Logtail Mgr里，做checkpoint要用到这些信息。

### Transaction Commands
   
  事务中每种写操作对应一个或多个command。WAL日志会记录事务中所有的command。

  | Operator      | Command        |
  | ------------- | -------------- |
  | DDL           | Update Catalog |
  | Insert        | Update Catalog |
  |               | Append         |
  | Delete        | Delete         |
  | Compact&Merge | Update Catalog |

#### Operators

DN支持建库，删库，建表，删表，更新表结构，插入，删除，同时后台会自动触发排序。更新操作被拆分成插入和删除。

1. DDL
   
   DDL包括建库，删库，建表，删表，更新表结构。DN在Catalog里记录了表和库的信息。内存里的catalog是一棵树，每个结点是一条catalog entry。catalog entry有4类，database，table，segment和block，其中segment和block是元数据，在插入数据和后台排序的时候会变更。每条database entry对应一个库，每条table entry对应一张表。每个DDL操作对应一条database/table entry，在WAL里记录成Update Catalog Command。
2. Insert
   
   新插入的数据记录在Append Command中。
   DN中的数据记录在block中，多个block组成一个segment。如果DN中没有足够的block或segment记录新插入的数据，就会新建一个。这些变化记录在Update Catalog Command中。
   大事务中，由CN直接把数据写入S3，DN只提交元数据。这样，Append Command中的数据不会很大。
3. Delete
   
   DN记录Delete发生的行号。读取时，先读所有插入过的数据，然后再减去这些行。事务中，同一个block上所有的删除合并起来，对应一个Delete Command。
4. Compact & Merge
   
   DN后台发起事务，把内存里的数据转存到s3上。把S3上的数据按主键排序，方便读的时候过滤。
   compact发生在一个block上，compact之后block内的数据是有序的。merge发生在segment里，会涉及多个block，merge之后整个segment内有序。
   compact/merge前后的数据不变，只改变元数据，删除旧的block/segment，创建新的block/segment。每次删除/创建对应一条Update Catalog Command。

#### Commands
  1. Update Catalog

     Catalog从上到下每层分别是database，table，segment和block。一条Updata Catalog Command对应一条Catalog Entry。每次ddl或者跟新元数据对应一条Update Catalog Command。Update Catalog Command包含Dest和EntryNode。
     ```
     +-------------------+
     |   Update Catalog  |
     +-------+-----------+
     | Dest | EntryNode |
     +-------+-----------+
     ```
     * Dest

       Dest是这条Command作用的位置，记录了对应结点和他的祖先结点的id。重启的时候会通过Dest，在Catalog上定位到操作的位置。
       | Type            | Dest                                       |
       | --------------- | ------------------------------------------- |
       | Update Database | database id                                 |
       | Update Table    | database id, table id                       |
       | Update Segment  | database id, table id, segment id           |
       | Update Block    | database id, table id, segment id, block id |
     * EntryNode
       * 每个EntryNode都记录了entry的创建时间和删除时间。如果entry没被删除，删除时间为0。如果当前事务正在创建或者删除，对应的时间为`UncommitTS`。
         ```
         +-------------------+
         |    Entry Node     |
         +---------+---------+
         | Create@ | Delete@ |
         +---------+---------+
         ```
       * 对于segment和block，Entry Node还记录了metaLoc，deltaLoc，分别是数据和删除记录在S3上的地址。
          ```
           +----------------------------------------+
           |               Entry Node               |
           +---------+---------+---------+----------+
           | Create@ | Delete@ | metaLoc | deltaLoc |
           +---------+---------+---------+----------+
          ```
       * 对于table，Entry Node还记录了表结构schema。
          ```
           +----------------------------+
           |         Entry Node         |
           +---------+---------+--------+
           | Create@ | Delete@ | schema |
           +---------+---------+--------+
          ```
  2. Append
   
     Append Command中记录了插入的数据和和这些数据的位置。
     ```
     +-------------------------------------------+
     |             Append Command                |
     +--------------+--------------+-   -+-------+
     | AppendInfo-1 | AppendInfo-2 | ... | Batch |
     +--------------+--------------+-   -+-------+
     ```
     * Batch是插入的数据
     * AppendInfo
       一个Append Data Command中的数据可能跨多个block。每个block对应一个Append Info，记录了数据在Command的Batch中的位置`pointer to data`，还有数据在block中的位置`destination`。
       ```
       +------------------------------------------------------------------------------+
       |                              AppendInfo                                      |
       +-----------------+------------------------------------------------------------+
       | pointer to data |                     destination                            |
       +--------+--------+-------+----------+------------+----------+--------+--------+
       | offset | length | db id | table id | segment id | block id | offset | length |
       +--------+--------+-------+----------+------------+----------+--------+--------+
       ```


  3. Delete Command
   
     每个Delete Command只包含一个block中的删除。
     ```
     +---------------------------+
     |      Delete Command       |
     +-------------+-------------+
     | Destination | Delete Mask |
     +-------------+-------------+
     ```
     * Destination记录Delete发生在哪个Block上。
     * Delete Mask记录删除掉的行号。