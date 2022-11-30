# Global Checkpoint

Global checkpoint 包含了给定时间戳t之前的所有catalog里的信息，包括系统表mo_database, mo_tables, mo_columns里的所有数据和其他系统表和用户表里的所有元数据。
Global checkpoint 会在dn重启和cn收集logtail的时候被消费。

## Global checkpoint entry
Global checkpoint是以batch的形式记录的。Global checkpoint entry里有三类batch: 一个记录entry内部元数据的batch，一些记录cn所需要的数据的batch，和一些记录cn不需要但是dn replay里需要的数据的batch.

### Metadata batch
   为了支持以table粒度获取checkpoint entry，meta batch里记录了每个用户表的metadata记录在entry里的位置。

### CN 需要的batch
   在cn收集logtail的时候被消费，格式和logtail里一样。
   1. DBInsert, DBDelete mo_database里新增和删除的行。
   2. TBLInsert, TBLDelete mo_tables里新增和删除的行。
   3. TBLColInsert, TBLColDelete mo_columns里新增和删除的行。
   4. BLKMetaInsert, BLKMetaDelete, BLKCNMetaInsert
   
      BLKMetaInsert, BLKMetaDelete 是用户表里新增和删除的元数据。BLKCNMetaInsert的内容和BLKMetaDelete一样，只是格式是Delete的，是为了做幂等。
      当以table粒度获取用户表的checkpoint entry时，只需要一部分batch，为了减少io, BLKMetaInsert, BLKMetaDelete, BLKCNMetaInsert会被拆成多个batch。
      拆batch的时候，会确保属于同一个表的元数据会记录在同一个batch。收集元数据时，记录完一个表后，如果当前的batch超过10000行，会新开一个batch记录接下来的元数据。

### DN replay时需要的batch
   dn重启的时候，额外需要一些信息，包括
   1. 每条记录相关的txn的start TS, prepare TS, end TS, log index
   2. block 或者 table所属的database id, table id, segment id
   3. segment相关的信息

## 生成global checkpoint
* 触发global checkpoint的要满足：
  1. 距离上次global checkpoint的时间超过了配置中的global-interval
  2. 从上次global/incremental checkpoint起，写事务数量超过了min-count
  3. 一旦满足上面两个条件，就会选定一个时间戳。等选定的时间戳之前的更改全部刷盘之后，就会开始做global checkpoint.

* 收集global checkpoint
  
    global checkpoint从catalog里收集。会扫描整个catalog里所有的database, table, segment, block，收集所有小于时间戳的记录, 记录在batch里。收集用户表的元数据的同时会记录table id和记录位置的对应关系，等扫描完整个catalog之后，把这些对应关系写入meta batch。

* 写入S3

    收集完的global checkpoint entry会写入S3, 一个global checkpoint对应一个object。global checkpoint的地址会被记录在checkpoint元数据里。每次checkpoint写入s3之后，所有的元数据也会写到s3上，包括新增的和之前所有的元数据。

* GC

    global checkpoint entry和最新的checkpoint metadata写入s3之后，会异步gc之前的global/incremental checkpoint和checkpoint元数据。

## 消费global checkpoint

1. replay in CN

   CN请求logtail的时候，如果cn have小于global checkpoint的时间戳，就会把global checkpoint在发给cn。选定的checkpoint的地址会放在CkpLocation字段里。
   根据CkpLocation读出checkpoint会有3次io: 1. 读object的元数据，2. 读checkpoint entry的元数据，确定batch的位置，3. 读出所有的batch。只有请求的表所在的batch会被读出来。
   相应的batch按格式填入entry, 然后apply到cn。

2. replay in DN
   1. 加载checkpoint
      共4次io: 1. 扫描ckp/并读取元数据所在的object的metadata。2. 读取checkpoint metadata。3. 根据checkpoint metadata里的文件名，并发读取每个checkpoint所在object的metadata。4. 读取所有的checkpoint。
   2. apply checkpoint
      按database, table, segment, block的顺序apply, 先apply append，再apply delete。Apply的时候，会按catalog entry的id做幂等，table和database不会按name去重。
   3. apply之后记录global checkpoint的TS，方便跳过wal里的日志。