set @stats="[3,7528422223,573384797164.000,0,1,247109,2]";
set @duration=309319808921;
select json_extract(@stats, '$[1]') val;
select json_extract(@stats, '$[2]') val;
select json_extract(@stats, '$[3]') val;
select json_extract(@stats, '$[4]') val;
select json_extract(@stats, '$[5]') val;
select json_extract(@stats, '$[6]') val;
select CAST(mo_cu(@stats, @duration) AS DECIMAL(32,6)) as cu, CAST(mo_cu(@stats, @duration, "total") AS DECIMAL(32,6)) cu_total, CAST(mo_cu(@stats, @duration, "cpu") AS DECIMAL(32,6)) cu_cpu, CAST(mo_cu(@stats, @duration, "mem") AS DECIMAL(32,6)) cu_mem, CAST(mo_cu(@stats, @duration, "ioin") AS DECIMAL(32,6)) cu_ioin, CAST(mo_cu(@stats, @duration, "ioout") AS DECIMAL(32,6)) cu_ioout, CAST(mo_cu(@stats, @duration, "network") AS DECIMAL(32,6)) cu_network;
select CAST(JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[1]')) * 3.45e-14 / 1.002678e-06  AS DECIMAL(32,6)) - CAST(mo_cu(@stats, @duration, "cpu") AS DECIMAL(32,6)) val;
select CAST(JSON_UNQUOTE(JSON_EXTRACT(@stats, '$[2]')) * 4.56e-24 * @duration / 1.002678e-06 AS DECIMAL(32,6)) - CAST(mo_cu(@stats, @duration, "mem") AS DECIMAL(32,6)) val;
select mo_cu('[1,1,2,3,4,5,0,0]', 0) val;
select mo_cu('[3,1,2,3,4,5,0,0]', 0) val;
-- old version, value: 0
select mo_cu('[4,1,2,3,4,5,6,7,8]', 0, 'iodelete') val;
select mo_cu('[4,1,2,3,4,5,6,7,8]', 0, 'iolist') val;
-- new version: v5.
-- new elem
-- index | op
--     9 | list
--    10 | delete
select CAST(mo_cu('[5,1,2,3,4,5,6,7,8,1,2]', 0, 'iolist') AS DECIMAL(32,4)) val;
select CAST(mo_cu('[5,1,2,3,4,5,6,7,8,1,2]', 0, 'iodelete') AS DECIMAL(32,4)) val;
