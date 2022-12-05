# Checkpoint

Checkpoint确定某一时间点之前的数据已经全部持久化到s3上，wal里的数据可以被truncate。
Checkpoint entry包含了一段时间内的所有catalog里的信息，包括系统表mo_database, mo_tables, mo_columns里的所有数据和其他表里的所有元数据。生成checkpoint时，用户表内的数据已经刷盘，所以checkpoint entry里不包含用户表里的真实数据。Checkpoint分两种，Incremental checkpoint和global checkpoint。Global checkpoint从0开始，覆盖之前所有的checkpoint。increment checkpoint从上个checkpoint的end+1开始，不包含之前checkpoint的数据。Incremental checkpoint的代价比global checkpoint小，比global checkpoint更频繁。
checkpoint 会在dn重启和cn收集logtail的时候被消费。

## Checkpoint entry
Checkpoint是以batch的形式记录的。checkpoint entry里有三类batch: 一个记录entry内部元数据的batch，一些记录cn所需要的数据的batch，和一些记录cn不需要但是dn replay时需要的数据的batch.

### Metadata batch
   为了支持以table粒度获取checkpoint entry，metadata batch里记录了每个用户表在entry里的位置，包括batch的编号，batch里的offset和length。在读取CN checkpoint的时候，可以通过用户表的table id迅速找到。

### CN 需要的batch
   这些batch在cn收集logtail的时候被消费，它们的格式和logtail里一样。
   1. DBInsert, DBDelete mo_database里新增和删除的行。
   2. TBLInsert, TBLDelete mo_tables里新增和删除的行。
   3. TBLColInsert, TBLColDelete mo_columns里新增和删除的行。
   4. BLKMetaInsert, BLKMetaDelete, BLKCNMetaInsert
   
      BLKMetaInsert, BLKMetaDelete 是用户表里新增和删除的元数据。BLKCNMetaInsert的内容和BLKMetaDelete一样，只是格式是Delete的，是为了做幂等。
      当以table粒度获取用户表的checkpoint entry时，只需要一部分batch，为了减少io, BLKMetaInsert, BLKMetaDelete, BLKCNMetaInsert会被拆成多个batch。
      拆batch的时候，会确保属于同一个表的元数据会记录在同一个batch。收集用户表的元数据时，记录完一个表后，如果当前的batch超过10000行，则新开一个batch记录接下来的元数据。

### DN replay时需要的batch
   这些batch里记录了dn重启的时候额外需要的信息，包括：
   1. 每条记录相关的txn的start TS, prepare TS, end TS, log index
   2. block 或者 table所属的database id, table id, segment id
   3. segment相关的信息
      CN不感知segment，segment的信息维护在DN里，重启DN的时候会用到。

### Checkpoint Metadata
   Checkpoint metadata记录了所有checkpoint entry在s3上的地址。在内存里有一份checkpoint metadata，在cn请求logtail的时候使用。在s3上的/ckp目录里也有一份，每次写checkpoint entry的同时会写checkpoint metadata文件，文件里包含所有的checkpoint entry的地址信息，能覆盖之前的checkpoint metadata，之前的文件会被异步清理掉。DN启动的时候会扫描ckp/目录下所有的文件，选取最新的checkpoint metadata文件来得到checkpoint entry的地址。

## 生成checkpoint
* 触发checkpoint要满足：
  1. 距离上次checkpoint的时间超过了配置中的interval（global-interval和incremental-interval）。
  2. 从上次global/incremental checkpoint起，写事务数量超过了min-count。
  3. 一旦满足上面两个条件，就会确定checkpoint的时间段。Global checkpoint从0开始，会覆盖之前所有的时间戳。incremental checkpoint从上个checkpoint的end+1开始。等选定的时间段之后会定时检查时间段内的写事务相关的数据是否刷盘，等到之前的更改全部刷盘之后，就会开始做checkpoint。
  4. 如果之前没有任何checkpoint，第一次生成的checkpoint会时global checkpoint而不是incremental checkpoint。

* 收集checkpoint
  
    checkpoint从catalog里收集。会扫描整个catalog里所有的database, table, segment, block，收集所有checkpoint时间段内的记录, 记录在batch里。收集用户表的元数据的同时会记录table id和记录位置的对应关系，等扫描完整个catalog之后，把这些对应关系写入metadata batch。

* 写入S3

    每次checkpoint会持久化checkpoint entry本身和checkpoint元数据。
    收集完的checkpoint entry会写入S3, 一个checkpoint对应一个object。checkpoint的地址会被记录在checkpoint元数据里。每次checkpoint写入s3之后，所有的元数据也会写到s3上，包括新增的和之前所有的元数据。

* GC

    global checkpoint entry会覆盖之前所有的checkpoint entry，做完global checkpoint后会安排GC之前的checkpoint entry。
    checkpoint metadata文件里包含了全部checkpoint的元数据，写完后会GC旧的checkpoint metadata文件，保证ckp/目录下只有1~2个文件。
    checkpoint entry能覆盖ts前所有wal里的数据，会对wal执行truncate。checkpoint之后，会根据end ts找到对应的wal lsn，truncate lsn之前的wal。

## 消费checkpoint

1. replay in CN

   1. 选择checkpoint
      只要checkpoint的时间段和cn需要的时间段有重合，就会选中。e.g. CN需要[35,55]，checkpoint[30,40]，checkpoint[40,50], checkpoint[50,60]会被选中。
   2. 传给CN
      选定的checkpoint的地址会放在CkpLocation字段里传给CN。
   3. 读取checkpoint
      根据CkpLocation读出checkpoint会有3次io: 1. 读object的元数据，2. 读checkpoint entry的元数据，确定batch的位置，3. 读出所有的batch。只有请求的表所在的batch会被读出来。
   4. apply到CN
      checkpoint的apply和logtail一样。相应的batch按格式填入entry, 然后apply到cn。

2. replay in DN
   1. 加载checkpoint
      共4次io: 1. 扫描ckp/并读取元数据所在的object的metadata。2. 读取checkpoint metadata。3. 根据checkpoint metadata里的文件名，并发读取每个checkpoint所在object的metadata。4. 读取所有的checkpoint。
   2. apply checkpoint
      按database, table, segment, block的顺序apply, 先apply append，再apply delete。Apply的时候，会按catalog entry的id做幂等，table和database不会按name去重。
   3. apply之后记录global checkpoint的TS，方便跳过wal里的日志。