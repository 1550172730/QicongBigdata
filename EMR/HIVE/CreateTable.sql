-- Hive table creation statement for e-commerce daily sales records
-- Table: ALLAN_SALE_ODS

CREATE EXTERNAL TABLE IF NOT EXISTS ALLAN_SALE_ODS (
    -- Primary keys and identifiers
    order_id STRING COMMENT '订单唯一标识ID',
    order_item_id STRING COMMENT '订单商品项ID',
    product_id STRING COMMENT '商品ID',
    user_id STRING COMMENT '用户ID',
    seller_id STRING COMMENT '卖家ID',

    -- Product information
    product_name STRING COMMENT '商品名称',
    product_category_id STRING COMMENT '商品类别ID',
    product_category_name STRING COMMENT '商品类别名称',
    brand_id STRING COMMENT '品牌ID',
    brand_name STRING COMMENT '品牌名称',

    -- Price and quantity information
    original_price DECIMAL(10,2) COMMENT '商品原价',
    actual_price DECIMAL(10,2) COMMENT '实际成交价',
    discount_amount DECIMAL(10,2) COMMENT '折扣金额',
    quantity INT COMMENT '购买数量',
    total_amount DECIMAL(10,2) COMMENT '订单总金额',

    -- Order details
    order_status STRING COMMENT '订单状态(待付款、已付款、已发货、已完成、已取消等)',
    payment_method STRING COMMENT '支付方式(支付宝、微信、银行卡等)',
    shipping_fee DECIMAL(10,2) COMMENT '运费',
    tax_amount DECIMAL(10,2) COMMENT '税费',

    -- Customer information
    user_province STRING COMMENT '用户所在省份',
    user_city STRING COMMENT '用户所在城市',
    user_age_group STRING COMMENT '用户年龄段',
    user_gender STRING COMMENT '用户性别',

    -- Promotion and marketing
    promotion_id STRING COMMENT '促销活动ID',
    coupon_id STRING COMMENT '优惠券ID',
    coupon_amount DECIMAL(10,2) COMMENT '优惠券金额',

    -- Timestamps
    order_time TIMESTAMP COMMENT '下单时间',
    payment_time TIMESTAMP COMMENT '支付时间',
    delivery_time TIMESTAMP COMMENT '发货时间',

    -- Source information
    source_channel STRING COMMENT '订单来源渠道(APP、网站、小程序等)',
    is_new_customer BOOLEAN COMMENT '是否新客户'
)
COMMENT '电商行业每日商品销售记录表'
PARTITIONED BY (dt STRING COMMENT '日期分区，格式为YYYY-MM-DD')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://allan-bigdata-test/ODS/allan_sales_ods/'
TBLPROPERTIES (
    'textfile.compress'='true',
    'compression.codec'='org.apache.hadoop.io.compress.SnappyCodec',
    'skip.header.line.count'='1');


-- Create Parquet Table
CREATE EXTERNAL TABLE IF NOT EXISTS ALLAN_Sale_Product (
    -- Primary keys and identifiers
    product_id STRING COMMENT '商品ID',
    daily_sale INT COMMENT '日销售额'
)
COMMENT '电商行业商品日销售额'
PARTITIONED BY (dt STRING COMMENT '日期分区，格式为YYYY-MM-DD')
STORED AS PARQUET
LOCATION 's3://allan-bigdata-test/DWD/allan_sale_product/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY');

-- Dimension table for product categories
-- Table: dim_product_category

CREATE TABLE IF NOT EXISTS dim_product_category (
    product_category_id STRING COMMENT '商品类别ID',
    product_category_name STRING COMMENT '商品类别名称',
    parent_category_id STRING COMMENT '父类别ID',
    category_level INT COMMENT '类别层级',
    category_description STRING COMMENT '类别描述',
    is_active BOOLEAN COMMENT '是否激活',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '商品类别维度表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Dimension table for user provinces
-- Table: dim_user_province

CREATE TABLE IF NOT EXISTS dim_user_province (
    province_id STRING COMMENT '省份ID',
    province_name STRING COMMENT '省份名称',
    region STRING COMMENT '所属区域(华东、华南、华北等)',
    capital_city STRING COMMENT '省会城市',
    population BIGINT COMMENT '人口数量',
    gdp DECIMAL(20,2) COMMENT 'GDP值',
    area DECIMAL(20,2) COMMENT '面积(平方公里)',
    is_municipality BOOLEAN COMMENT '是否直辖市',
    is_special_administrative_region BOOLEAN COMMENT '是否特别行政区',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '用户省份维度表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Dimension table for promotions
-- Table: dim_promotion

CREATE TABLE IF NOT EXISTS dim_promotion (
    promotion_id STRING COMMENT '促销活动ID',
    promotion_name STRING COMMENT '促销活动名称',
    promotion_type STRING COMMENT '促销类型(满减、折扣、赠品等)',
    start_time TIMESTAMP COMMENT '开始时间',
    end_time TIMESTAMP COMMENT '结束时间',
    discount_rate DECIMAL(5,2) COMMENT '折扣率',
    discount_amount DECIMAL(10,2) COMMENT '折扣金额',
    minimum_order_amount DECIMAL(10,2) COMMENT '最低订单金额',
    maximum_discount DECIMAL(10,2) COMMENT '最高折扣金额',
    applicable_products STRING COMMENT '适用商品范围',
    applicable_categories STRING COMMENT '适用类别范围',
    is_active BOOLEAN COMMENT '是否激活',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '促销活动维度表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
