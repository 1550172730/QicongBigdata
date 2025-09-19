#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成脚本：为ALLAN_SALE_ODS表生成模拟业务数据
- ALLAN_SALE_ODS: 100万条数据
"""

import pandas as pd
import numpy as np
import random
import string
import uuid
from datetime import datetime, timedelta
import os

# 设置随机种子，确保结果可重现
np.random.seed(42)
random.seed(42)

# 输出目录
OUTPUT_DIR = "data_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 生成的记录数
FACT_RECORDS = 1000000

def generate_id():
    """生成唯一ID"""
    return str(uuid.uuid4())

def generate_timestamp(start_date="2023-01-01", end_date="2023-12-31"):
    """生成随机时间戳"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 24 * 60 * 60 - 1)
    random_date = start + timedelta(days=random_days, seconds=random_seconds)
    return random_date

def load_dimension_data():
    """加载维度表数据"""
    print("加载维度表数据...")
    
    # 检查维度表数据文件是否存在
    dim_files = {
        "product_category": f"{OUTPUT_DIR}/dim_product_category.csv",
        "user_province": f"{OUTPUT_DIR}/dim_user_province.csv",
        "promotion": f"{OUTPUT_DIR}/dim_promotion.csv"
    }
    
    # 检查文件是否存在，如果不存在则提示用户先运行相应的脚本
    for name, file_path in dim_files.items():
        if not os.path.exists(file_path):
            print(f"错误: {file_path} 不存在。请先运行 generate_dim_{name}.py 生成维度表数据。")
            return None, None, None
    
    # 加载维度表数据
    dim_product_category = pd.read_csv(dim_files["product_category"], sep='\t')
    dim_user_province = pd.read_csv(dim_files["user_province"], sep='\t')
    dim_promotion = pd.read_csv(dim_files["promotion"], sep='\t')
    
    print(f"已加载维度表数据: 商品类别({len(dim_product_category)}条), 用户省份({len(dim_user_province)}条), 促销活动({len(dim_promotion)}条)")
    
    return dim_product_category, dim_user_province, dim_promotion

def generate_allan_sale_ods(dim_product_category, dim_user_province, dim_promotion):
    """生成销售事实表数据"""
    print(f"生成销售事实表数据（{FACT_RECORDS}条）...")
    
    # 从维度表中提取ID列表，用于生成引用完整性的数据
    product_category_ids = dim_product_category["product_category_id"].tolist()
    product_category_names = dim_product_category["product_category_name"].tolist()
    province_ids = dim_user_province["province_id"].tolist()
    province_names = dim_user_province["province_name"].tolist()
    promotion_ids = dim_promotion["promotion_id"].tolist()
    
    # 生成商品ID和名称
    product_ids = [f"PROD{i:06d}" for i in range(1, 10001)]  # 1万个商品
    product_names = []
    for _ in range(10000):
        cat_idx = random.randint(0, len(product_category_names) - 1)
        cat_name = product_category_names[cat_idx]
        product_names.append(f"{cat_name}{random.choice(['精选', '优选', '特惠', '尊享', ''])}商品{random.randint(1, 1000)}")
    
    # 生成用户ID
    user_ids = [f"USER{i:06d}" for i in range(1, 100001)]  # 10万个用户
    
    # 生成卖家ID
    seller_ids = [f"SELLER{i:04d}" for i in range(1, 1001)]  # 1千个卖家
    
    # 生成品牌ID和名称
    brand_ids = [f"BRAND{i:03d}" for i in range(1, 201)]  # 200个品牌
    brand_names = [f"品牌{i}" for i in range(1, 201)]
    
    # 订单状态和支付方式
    order_statuses = ["待付款", "已付款", "已发货", "已完成", "已取消", "已退款"]
    payment_methods = ["支付宝", "微信", "银行卡", "信用卡", "货到付款", "余额支付"]
    
    # 用户年龄段和性别
    age_groups = ["18岁以下", "18-24岁", "25-34岁", "35-44岁", "45-54岁", "55岁以上"]
    genders = ["男", "女", "未知"]
    
    # 来源渠道
    source_channels = ["APP", "网站", "小程序", "第三方平台", "线下门店"]
    
    # 生成分区日期列表（最近一年的日期）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    partition_dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(366)]
    
    # 批量生成数据
    batch_size = 100000  # 每批生成10万条数据
    batches = FACT_RECORDS // batch_size
    remainder = FACT_RECORDS % batch_size
    
    all_data = []
    
    for batch in range(batches + (1 if remainder > 0 else 0)):
        print(f"生成批次 {batch+1}/{batches + (1 if remainder > 0 else 0)}...")
        
        # 确定当前批次的大小
        current_batch_size = batch_size if batch < batches else remainder
        
        # 生成订单ID和订单项ID
        order_ids = [f"ORDER{batch*batch_size+i:08d}" for i in range(1, current_batch_size + 1)]
        order_item_ids = [f"ITEM{batch*batch_size+i:09d}" for i in range(1, current_batch_size + 1)]
        
        # 生成随机数据
        data = []
        for i in range(current_batch_size):
            # 随机选择商品
            product_idx = random.randint(0, len(product_ids) - 1)
            product_id = product_ids[product_idx]
            product_name = product_names[product_idx]
            
            # 随机选择类别
            cat_idx = random.randint(0, len(product_category_ids) - 1)
            product_category_id = product_category_ids[cat_idx]
            product_category_name = product_category_names[cat_idx]
            
            # 随机选择品牌
            brand_idx = random.randint(0, len(brand_ids) - 1)
            brand_id = brand_ids[brand_idx]
            brand_name = brand_names[brand_idx]
            
            # 随机选择省份
            province_idx = random.randint(0, len(province_ids) - 1)
            user_province = province_names[province_idx]
            
            # 随机选择促销活动
            use_promotion = random.random() < 0.3  # 30%的订单使用促销
            promotion_id = random.choice(promotion_ids) if use_promotion else ""
            
            # 随机生成价格和数量
            original_price = round(random.uniform(10, 5000), 2)
            discount_rate = round(random.uniform(0.5, 1), 2) if use_promotion else 1
            actual_price = round(original_price * discount_rate, 2)
            discount_amount = round(original_price - actual_price, 2)
            quantity = random.randint(1, 5)
            total_amount = round(actual_price * quantity, 2)
            
            # 随机生成运费和税费
            shipping_fee = 0 if total_amount > 99 else round(random.uniform(5, 20), 2)
            tax_amount = round(total_amount * 0.13, 2)  # 假设13%的税率
            
            # 随机生成优惠券信息
            use_coupon = random.random() < 0.2  # 20%的订单使用优惠券
            coupon_id = f"COUPON{random.randint(1, 1000):04d}" if use_coupon else ""
            coupon_amount = round(random.uniform(5, 50), 2) if use_coupon else 0
            
            # 调整实际价格和总金额
            if use_coupon:
                total_amount = max(0.01, round(total_amount - coupon_amount, 2))
            
            # 随机生成订单时间
            partition_date = random.choice(partition_dates)
            order_time = datetime.strptime(partition_date, "%Y-%m-%d") + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # 根据订单状态生成其他时间
            order_status = random.choice(order_statuses)
            
            if order_status == "待付款":
                payment_time = None
                delivery_time = None
            elif order_status == "已付款":
                payment_time = order_time + timedelta(minutes=random.randint(5, 120))
                delivery_time = None
            elif order_status in ["已发货", "已完成"]:
                payment_time = order_time + timedelta(minutes=random.randint(5, 120))
                delivery_time = payment_time + timedelta(hours=random.randint(1, 72))
            else:  # 已取消、已退款
                payment_time = order_time + timedelta(minutes=random.randint(5, 120)) if random.random() < 0.5 else None
                delivery_time = None
            
            # 随机生成是否新客户
            is_new_customer = random.random() < 0.1  # 10%的订单来自新客户
            
            data.append({
                # 主键和标识符
                "order_id": order_ids[i],
                "order_item_id": order_item_ids[i],
                "product_id": product_id,
                "user_id": random.choice(user_ids),
                "seller_id": random.choice(seller_ids),
                
                # 商品信息
                "product_name": product_name,
                "product_category_id": product_category_id,
                "product_category_name": product_category_name,
                "brand_id": brand_id,
                "brand_name": brand_name,
                
                # 价格和数量信息
                "original_price": original_price,
                "actual_price": actual_price,
                "discount_amount": discount_amount,
                "quantity": quantity,
                "total_amount": total_amount,
                
                # 订单详情
                "order_status": order_status,
                "payment_method": random.choice(payment_methods),
                "shipping_fee": shipping_fee,
                "tax_amount": tax_amount,
                
                # 客户信息
                "user_province": user_province,
                "user_city": f"{user_province}市" if "省" in user_province else user_province,
                "user_age_group": random.choice(age_groups),
                "user_gender": random.choice(genders),
                
                # 促销和营销
                "promotion_id": promotion_id,
                "coupon_id": coupon_id,
                "coupon_amount": coupon_amount,
                
                # 时间戳
                "order_time": order_time,
                "payment_time": payment_time,
                "delivery_time": delivery_time,
                
                # 来源信息
                "source_channel": random.choice(source_channels),
                "is_new_customer": is_new_customer,
                
                # 分区字段
                "dt": partition_date
            })
        
        # 添加到总数据中
        all_data.extend(data)
        
        # 如果数据量太大，分批保存
        if len(all_data) >= 500000:
            df_chunk = pd.DataFrame(all_data)
            # 保存到CSV文件
            chunk_file = f"{OUTPUT_DIR}/allan_sale_ods_part_{batch}.csv"
            df_chunk.to_csv(chunk_file, index=False, sep='\t')
            print(f"已保存{len(df_chunk)}条数据到{chunk_file}")
            all_data = []
    
    # 保存剩余数据
    if all_data:
        df_final = pd.DataFrame(all_data)
        final_file = f"{OUTPUT_DIR}/allan_sale_ods_final.csv"
        df_final.to_csv(final_file, index=False, sep='\t')
        print(f"已保存{len(df_final)}条数据到{final_file}")
    
    print(f"已完成销售事实表{FACT_RECORDS}条数据的生成")

def main():
    """主函数"""
    print("开始生成销售事实表数据...")
    
    # 加载维度表数据
    dim_product_category, dim_user_province, dim_promotion = load_dimension_data()
    
    # 检查是否成功加载维度表数据
    if dim_product_category is None or dim_user_province is None or dim_promotion is None:
        print("错误: 无法加载维度表数据，请先运行维度表数据生成脚本。")
        return
    
    # 生成事实表数据
    generate_allan_sale_ods(dim_product_category, dim_user_province, dim_promotion)
    
    print(f"销售事实表数据已生成完成，保存在{OUTPUT_DIR}目录下")

if __name__ == "__main__":
    main()