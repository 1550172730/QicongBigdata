#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成脚本：为dim_promotion表生成模拟业务数据
- dim_promotion: 50条数据
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
DIM_RECORDS = 50

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

def generate_dim_promotion():
    """生成促销活动维度表数据"""
    print("生成促销活动维度表数据...")
    
    promotion_types = ["满减", "折扣", "赠品", "秒杀", "限时特价", "套装优惠", "会员专享", "首单优惠"]
    promotion_names = [
        "双11狂欢节", "618年中大促", "新年特惠", "春节大礼包", "暑期大促", "开学季特惠",
        "感恩节特卖", "圣诞节促销", "情人节特惠", "母亲节献礼", "父亲节特惠", "儿童节促销",
        "周年庆典", "品牌日", "会员日", "新品首发", "清仓特卖", "限时闪购"
    ]
    
    applicable_categories = [
        "全品类", "电子产品", "服装", "家居", "食品", "美妆", "图书", "运动", "玩具", "母婴", "汽车用品"
    ]
    
    data = []
    for i in range(1, DIM_RECORDS + 1):
        # 生成促销活动名称
        if i <= len(promotion_names):
            name = promotion_names[i-1]
        else:
            name = f"{random.choice(promotion_names)}第{i}期"
        
        # 生成促销类型
        promo_type = random.choice(promotion_types)
        
        # 生成开始和结束时间
        start_time = generate_timestamp("2023-01-01", "2023-11-30")
        end_time = start_time + timedelta(days=random.randint(3, 30))
        
        # 根据促销类型生成不同的折扣信息
        if promo_type == "折扣":
            discount_rate = round(random.uniform(0.1, 0.9), 2)  # 10%-90%折扣
            discount_amount = 0
        elif promo_type == "满减":
            discount_rate = 1
            discount_amount = random.choice([10, 20, 50, 100, 200, 500])
        else:
            discount_rate = 1
            discount_amount = random.choice([0, 10, 20, 50])
        
        # 最低订单金额
        minimum_order_amount = random.choice([0, 50, 100, 200, 500, 1000])
        
        # 最高折扣金额
        maximum_discount = random.choice([0, 100, 200, 500, 1000, 2000])
        
        # 适用范围
        applicable_cats = random.choice(applicable_categories)
        applicable_products = "全部" if applicable_cats == "全品类" else f"部分{applicable_cats}"
        
        data.append({
            "promotion_id": f"PROMO{i:03d}",
            "promotion_name": name,
            "promotion_type": promo_type,
            "start_time": start_time,
            "end_time": end_time,
            "discount_rate": discount_rate,
            "discount_amount": discount_amount,
            "minimum_order_amount": minimum_order_amount,
            "maximum_discount": maximum_discount,
            "applicable_products": applicable_products,
            "applicable_categories": applicable_cats,
            "is_active": start_time <= datetime.now() <= end_time,
            "create_time": start_time - timedelta(days=random.randint(10, 30)),
            "update_time": start_time - timedelta(days=random.randint(1, 9))
        })
    
    # 转换为DataFrame并保存
    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/dim_promotion.csv", index=False, sep='\t')
    print(f"已生成{len(df)}条促销活动维度表数据")
    return df

def main():
    """主函数"""
    print("开始生成促销活动维度表数据...")
    
    # 生成维度表数据
    dim_promotion_df = generate_dim_promotion()
    
    print(f"促销活动维度表数据已生成完成，保存在{OUTPUT_DIR}目录下")

if __name__ == "__main__":
    main()