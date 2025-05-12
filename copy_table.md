# Copy Table

## 需求

拷贝一张表的数据到指定目录。
从拷贝出来的文件，在另一个mo实例上应用数据。
应用数据的时候会完全按照原来的表。
内存中的数据会在mnode里，刷盘的数据会在磁盘上。object id不会改变。
保留object entry和anode上的commitTS，startTS设置成commitTS.prev()。

## 流程

### 拷贝表

```
select mo_ctl('dn','inspect','copy-table -d 272515 -t 272516 -o t1');

```

* -d database id
* -t table id
* -o 目录，这个目录在mo-data/shared下，e.g.如果设置-o t1，拷贝出来的数据会在mo-data/shared/t1下。

### 修改配置

目标MO要打开EnableApplyTableData,在tn.toml里配置：

```
[debug]
enable-apply-table-data = true
```

目标MO的fileservice配置要和原来的一样，
如果从s3上拷贝文件，在tn.toml和cn.toml里配置：
```
[[fileservice]]
name = "SHARED"
backend = "S3"
[fileservice.s3]
endpoint = "DISK"
bucket = "mo-data/shared"
```

必须关闭checkpoint，不然从客户端读数据的时候会缺数据，在dn.toml里配置：
```
[tn.Ckp]
flush-interval = "60s"
min-count = 1000000
scan-interval = "5s"
incremental-interval = "180000s"
global-min-count = 60
```

### 应用数据

```
select mo_ctl('dn','inspect','apply-table-data -d test2 -t t1 -o t1');
```

*-d database name，如果database不存在就建一个，如果存在就用这个database。
* -t table name，如果table name存在会报错。
* -o 目录，这个目录在mo-data/shared下，e.g.如果设置-o t1，就从mo-data/shared/t1下读取数据。
