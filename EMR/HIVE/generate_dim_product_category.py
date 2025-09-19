#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成脚本：为dim_product_category表生成模拟业务数据
- dim_product_category: 50条数据
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

def generate_dim_product_category():
    """生成商品类别维度表数据"""
    print("生成商品类别维度表数据...")
    
    # 主要商品类别
    main_categories = [
        "电子产品", "服装", "家居", "食品", "美妆", "图书", "运动", "玩具", 
        "母婴", "汽车用品"
    ]
    
    # 子类别
    sub_categories = {
        "电子产品": ["手机", "电脑", "相机", "耳机", "平板"],
        "服装": ["男装", "女装", "童装", "内衣", "鞋子"],
        "家居": ["家具", "厨具", "床上用品", "装饰品", "灯具"],
        "食品": ["零食", "饮料", "生鲜", "调味品", "酒水"],
        "美妆": ["护肤品", "彩妆", "香水", "美发", "美甲"],
        "图书": ["小说", "教育", "漫画", "杂志", "工具书"],
        "运动": ["健身器材", "运动服装", "户外装备", "球类", "游泳用品"],
        "玩具": ["积木", "娃娃", "电动玩具", "桌游", "益智玩具"],
        "母婴": ["奶粉", "尿布", "婴儿服装", "婴儿车", "玩具"],
        "汽车用品": ["车载电子", "内饰", "外饰", "维修工具", "清洁用品"]
    }
    
    data = []
    category_id = 1
    
    # 生成主类别
    for main_cat in main_categories:
        data.append({
            "product_category_id": f"CAT{category_id:03d}",
            "product_category_name": main_cat,
            "parent_category_id": "NULL",
            "category_level": 1,
            "category_description": f"{main_cat}类别包含多种{main_cat}相关商品",
            "is_active": True,
            "create_time": generate_timestamp("2022-01-01", "2022-12-31"),
            "update_time": generate_timestamp("2023-01-01", "2023-06-30")
        })
        parent_id = f"CAT{category_id:03d}"
        category_id += 1
        
        # 生成子类别
        for sub_cat in sub_categories[main_cat]:
            if category_id <= DIM_RECORDS:
                data.append({
                    "product_category_id": f"CAT{category_id:03d}",
                    "product_category_name": sub_cat,
                    "parent_category_id": parent_id,
                    "category_level": 2,
                    "category_description": f"{sub_cat}是{main_cat}的子类别",
                    "is_active": random.choice([True, True, True, False]),  # 75%概率为活跃状态
                    "create_time": generate_timestamp("2022-01-01", "2022-12-31"),
                    "update_time": generate_timestamp("2023-01-01", "2023-06-30")
                })
                category_id += 1
    
    # 确保正好生成50条记录
    while len(data) < DIM_RECORDS:
        # 随机选择一个主类别作为父类别
        parent_cat = random.choice(data[:10])
        parent_id = parent_cat["product_category_id"]
        main_cat = parent_cat["product_category_name"]
        
        # 生成一个新的子类别名称
        sub_cat = f"{main_cat}特殊系列{len(data) - DIM_RECORDS + 1}"
        
        data.append({
            "product_category_id": f"CAT{category_id:03d}",
            "product_category_name": sub_cat,
            "parent_category_id": parent_id,
            "category_level": 2,
            "category_description": f"{sub_cat}是{main_cat}的特殊子类别",
            "is_active": random.choice([True, True, False]),  # 66%概率为活跃状态
            "create_time": generate_timestamp("2022-01-01", "2022-12-31"),
            "update_time": generate_timestamp("2023-01-01", "2023-06-30")
        })
        category_id += 1
    
    # 转换为DataFrame并保存
    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/dim_product_category.csv", index=False, sep='\t')
    print(f"已生成{len(df)}条商品类别维度表数据")
    return df

def main():
    """主函数"""
    print("开始生成商品类别维度表数据...")
    
    # 生成维度表数据
    dim_product_category_df = generate_dim_product_category()
    
    print(f"商品类别维度表数据已生成完成，保存在{OUTPUT_DIR}目录下")

if __name__ == "__main__":
    main()