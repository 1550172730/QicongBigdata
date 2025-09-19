-- Hive命令：加载分区信息

-- 方法1：使用MSCK REPAIR TABLE命令
-- 这个命令会扫描表的存储位置，发现新的分区并将其添加到Hive的元数据中
-- 适用于分区目录已经存在但Hive元数据中没有相应记录的情况
MSCK REPAIR TABLE ALLAN_SALE_ODS;

-- 方法2：使用ALTER TABLE ADD PARTITION命令
-- 这个命令可以手动添加特定的分区
-- 适用于需要明确指定分区位置的情况
ALTER TABLE ALLAN_SALE_ODS ADD PARTITION (dt='2025-07-14') 
LOCATION 's3://allan-bigdata-test/ODS/allan_sales_ods/dt=2025-07-14/';

-- 方法3：使用ALTER TABLE RECOVER PARTITIONS命令（Hive 4.0.0及以上版本）
-- 这是MSCK REPAIR TABLE的替代命令，性能更好
-- ALTER TABLE ALLAN_SALE_ODS RECOVER PARTITIONS;

-- 注意：
-- 1. MSCK REPAIR TABLE在处理大量分区时可能会很慢
-- 2. 如果分区目录不存在，ADD PARTITION会创建目录
-- 3. 对于外部表，删除分区只会删除元数据，不会删除实际数据
-- 4. 可以使用SHOW PARTITIONS命令查看表的所有分区
-- SHOW PARTITIONS ALLAN_SALE_ODS;