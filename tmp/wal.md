
# MatrixOne WAL设计解析

为了保证持久性，事务要在提交前持久化。一般内存里会把这些更改更新到memtable中的block中，每个block对应Storage中的一页。如果重写发生更新的页，开销很大，不能保证提交性能。WAL(Write Ahead Log)只记录了事务更改的地方，类似于在哪个blk中增加了哪行。提交事务时不用更新重写整页，只持久化WAL，加速了事务提交。提交之后再异步地更新那些脏页，对应的WAL就能销毁了。MatrixOne的WAL是物理日志，记录每行更新发生的位置，而不是sql，这样，每次回放出来，数据不仅在逻辑上相同，底层的组织结构也是一样的。

下面是本文目录概览：
1. Commit Pipeline
2. Checkpoint
3. Driver & Log Backend
4. Group Commit
5. 处理Log Backend的乱序LSN
6. MatrixOne中WAL的具体格式

## Commit Pipeline

提交之前DN要更新memtable，持久化WAL，执行这些任务的耗时决定了提交的性能。 持久化WAL涉及到IO，比较耗时。TAE中采用commit pipeline，异步持久化WAL，不阻塞内存中的更新。事务提交的流程是：
1. 把更改更新到memtable中
   
   事务进入commit pipeline之前，先并发的更新memtable，事务之间互相不阻塞。这时这些更改的状态为未提交，还不可见。
2. 进入commit pipeline检查冲突
3. 持久化WAL
   
   持久化WAL是异步的，队列里只通知Log Backend写入WAL就立刻返回，不用等待写入成功，这样不会阻塞后续其他事务。Log Backend中支持并发写入，不用等待之前的事务。Log Backend中同时处理一批事务，通过Group Commit加速持久化。
4. 更新memtable中的状态使事务可见
   
   事务按照进队列的顺序依次更新状态，这样事务可见的顺序和队列里各个操作的顺序是一致的。

![](./image/wal_8.jpg)

TAE中事务在进入提交队列前更新Memtable和Catalog，在提交pipeline中批量写WAL，然后快速地标记状态使更改可见完成提交。
## Checkpoint

DN会选一个合适的时间戳作为checkpoint，然后扫描时间戳之前的修改。

![](./image/wal_10.jpg)

DN先转存DML修改。DML更改存在Memtable中的各个block中。Logtail Mgr中记录着每个事务改动了哪些block。DN在Logtail Mgr上扫描[t0,t1]之间的事务，发起后台事务把这些block转存到S3上，在元数据中记录地址。这样，所有t1前提交的DML更改都能通过元数据中的地址查到。为了及时做checkpoint，控制WAL的大小，哪怕区间中block只改动了一行，也需要转存。

![](./image/wal_11.jpg)

然后扫描Catalog，收集[t0,t1]之间的DDL和元数据更改，转存到S3上。Logtail Mgr中存了每个事务对应的WAL LSN。根据时间戳，找到t1前最后一个事务，然后通知Log Backend清理这个LSN之前的所有日志。
## Driver & Log Backend
MatrixOne的WAL能写在各种Log Backend中。最初使用的Log Backend基于本地文件系统。为了分布式特性，我们自研了高可靠低延迟Log Service作为新的Log Backend。Driver层被抽象出来，适配不同的Log Backend。

   Driver需要适配出这些接口：
  1. Append，提交事务时异步地Append WAL
   ```
    Append(entry) (Lsn, error)
   ```
  2. Read，重启时批量读取WAL
   ```
	Read(Lsn, maxSize) (entry, Lsn, error)
   ```
  3. Truncate，做checkpoint时销毁旧的WAL
   ```
	Truncate(lsn Lsn) error
   ```

  经过适配，各种消息队列（e.g. Kafka）也能作为Log Backend。


## Group Commit

  持久化WAL涉及到IO，非常耗时，经常是提交耗时的瓶颈。为了节省时间，TAE中批量向Log Backend中写入WAL。比如，在文件系统中fsync耗时很久。如果每条WAL都fsync，会耗费大量时间。基于文件系统的Log Backend中，多个WAL写完后统一只做一次fsync，这些WAL刷盘的时间成本之和近似一条WAL刷盘的时间。
  
![](./image/wal_9.jpg)

Log Service中支持并发写入，各条WAL刷盘的时间可以重合，这也能缩短写WAL的总时间，提高了提交的并发。

## 处理Log Backend的乱序LSN
Driver和Log Backend中各维护了一套LSN。Driver向Log Backend并发写入WAL时，写入成功的顺序和发出请求的顺序不一致，导致Log Backend中产生LSN是乱序的。销毁WAL和重启的时候要处理这些乱序LSN。Driver中会维护一个map，记录两套LSN的对应关系。为了保证Log Backend中的LSN基本有序，乱序的跨度不太大，Driver中维持了一个窗口，如果有很早的WAL正在写入还未成功，会停止向Log Backend写入新的WAL。举个例子，如果窗口的长度是7，图中的LSN为13的WAL还未返回，会阻塞住LSN大于等于20的WAL。

![](./image/wal_13.jpg)

Log Backend中通过truncate操作销毁日志，销毁指定LSN之前的所有WAL。这个LSN之前的WAL所对应的Driver LSN都要小于Driver中的truncate点。比如图中Driver里truncate到7，这条WAL对应Log Backend中的11，但是Log Backend中5，6，7，10对应的Driver LSN都大于7，不能被truncate。Log Backend只能truncate 4。

重启时，会跳过开始和末尾哪些不连续的WAL。如果Log Backend中写到14时，整个机器断电了，重启时会根据上次的truncate信息过滤掉开头8，9，11。等读完所有的WAL发现6，14的Driver LSN和其他的WAL不连续，就丢弃末尾的6和14。


## MatrixOne中WAL的具体格式

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

##
WAL在存储系统中应用得十分广泛。WAL减少掉每次事务写脏页的操作，是一种更高效的模式。WAL内部还有很多执行细节，我们会不断调整，不断优化。
