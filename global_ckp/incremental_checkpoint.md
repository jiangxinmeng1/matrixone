# Incremental Checkpoint

Incremental checkpoint和global checkpoint相似，区别是increment checkpoint从上个checkpoint的end开始，不包含之前checkpoint的数据。Incremental checkpoint的代价比global checkpoint小，比global checkpoint更频繁。

## Incremental checkpoint entry

Incremental checkpoint entry的内容和格式和global checkpoint entry相同。

## 生成incremental checkpoint

* 触发incremental checkpoint要满足：
  1. 距离上次global/checkpoint checkpoint的时间超过了配置中的incremental-interval
  2. 从上次global/incremental checkpoint起，写事务数量超过了min-count
  3. 一旦满足上面两个条件，就会选定一个时间戳。等选定的时间戳之前的更改全部刷盘之后，就会开始做incremental checkpoint。

* incremental checkpoint的收集和持久化和global checkpoint相同。

  每次checkpoint会产生两种数据：checkpoint entry本身和checkpoint metadata。其中checkpoint metadata包含checkpoint entry的start timestamp，end timestamp和checkpoint entry的地址。checkpoint metadata持久化在s3上ckp/目录下，当DN重启时，会通过扫描ckp/目录得到checkpoint metadata。每个metadata文件包含全量的checkpoint metadata,能覆盖之前所有的metadata文件，所以每写入一个新的metadata文件，都会安排gc之前的文件，这样能保证ckp/目录下只有1~2个文件。

* GC
  incremental checkpoint不会覆盖之前的checkpoint entry，所以不会gc checkpoint。incremental checkpoint中持久化的checkpoint metadata包含之前所有checkpoint的元数据，所以持久化成功后就会开始gc之前的checkpoint metadata。incremental checkpoint也能覆盖wal里的数据，会触发truncate。

## 消费incremental checkpoint

CN DN中消费incremental checkpoint的方式和global checkpoint相同。
