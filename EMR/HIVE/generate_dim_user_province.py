#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据生成脚本：为dim_user_province表生成模拟业务数据
- dim_user_province: 50条数据
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

def generate_dim_user_province():
    """生成用户省份维度表数据"""
    print("生成用户省份维度表数据...")
    
    # 中国省份数据
    provinces = [
        {"name": "北京市", "region": "华北", "capital": "北京", "is_municipality": True, "is_special": False},
        {"name": "天津市", "region": "华北", "capital": "天津", "is_municipality": True, "is_special": False},
        {"name": "河北省", "region": "华北", "capital": "石家庄", "is_municipality": False, "is_special": False},
        {"name": "山西省", "region": "华北", "capital": "太原", "is_municipality": False, "is_special": False},
        {"name": "内蒙古自治区", "region": "华北", "capital": "呼和浩特", "is_municipality": False, "is_special": False},
        {"name": "辽宁省", "region": "东北", "capital": "沈阳", "is_municipality": False, "is_special": False},
        {"name": "吉林省", "region": "东北", "capital": "长春", "is_municipality": False, "is_special": False},
        {"name": "黑龙江省", "region": "东北", "capital": "哈尔滨", "is_municipality": False, "is_special": False},
        {"name": "上海市", "region": "华东", "capital": "上海", "is_municipality": True, "is_special": False},
        {"name": "江苏省", "region": "华东", "capital": "南京", "is_municipality": False, "is_special": False},
        {"name": "浙江省", "region": "华东", "capital": "杭州", "is_municipality": False, "is_special": False},
        {"name": "安徽省", "region": "华东", "capital": "合肥", "is_municipality": False, "is_special": False},
        {"name": "福建省", "region": "华东", "capital": "福州", "is_municipality": False, "is_special": False},
        {"name": "江西省", "region": "华东", "capital": "南昌", "is_municipality": False, "is_special": False},
        {"name": "山东省", "region": "华东", "capital": "济南", "is_municipality": False, "is_special": False},
        {"name": "河南省", "region": "华中", "capital": "郑州", "is_municipality": False, "is_special": False},
        {"name": "湖北省", "region": "华中", "capital": "武汉", "is_municipality": False, "is_special": False},
        {"name": "湖南省", "region": "华中", "capital": "长沙", "is_municipality": False, "is_special": False},
        {"name": "广东省", "region": "华南", "capital": "广州", "is_municipality": False, "is_special": False},
        {"name": "广西壮族自治区", "region": "华南", "capital": "南宁", "is_municipality": False, "is_special": False},
        {"name": "海南省", "region": "华南", "capital": "海口", "is_municipality": False, "is_special": False},
        {"name": "重庆市", "region": "西南", "capital": "重庆", "is_municipality": True, "is_special": False},
        {"name": "四川省", "region": "西南", "capital": "成都", "is_municipality": False, "is_special": False},
        {"name": "贵州省", "region": "西南", "capital": "贵阳", "is_municipality": False, "is_special": False},
        {"name": "云南省", "region": "西南", "capital": "昆明", "is_municipality": False, "is_special": False},
        {"name": "西藏自治区", "region": "西南", "capital": "拉萨", "is_municipality": False, "is_special": False},
        {"name": "陕西省", "region": "西北", "capital": "西安", "is_municipality": False, "is_special": False},
        {"name": "甘肃省", "region": "西北", "capital": "兰州", "is_municipality": False, "is_special": False},
        {"name": "青海省", "region": "西北", "capital": "西宁", "is_municipality": False, "is_special": False},
        {"name": "宁夏回族自治区", "region": "西北", "capital": "银川", "is_municipality": False, "is_special": False},
        {"name": "新疆维吾尔自治区", "region": "西北", "capital": "乌鲁木齐", "is_municipality": False, "is_special": False},
        {"name": "香港特别行政区", "region": "华南", "capital": "香港", "is_municipality": False, "is_special": True},
        {"name": "澳门特别行政区", "region": "华南", "capital": "澳门", "is_municipality": False, "is_special": True},
        {"name": "台湾省", "region": "华东", "capital": "台北", "is_municipality": False, "is_special": False}
    ]
    
    data = []
    for i, province in enumerate(provinces, 1):
        if i <= DIM_RECORDS:
            # 生成随机人口数据（单位：万人）
            population = random.randint(500, 10000) * 10000
            
            # 生成随机GDP数据（单位：亿元）
            gdp = round(random.uniform(1000, 110000), 2)
            
            # 生成随机面积数据（单位：平方公里）
            area = round(random.uniform(1000, 1660000), 2)
            
            data.append({
                "province_id": f"PROV{i:02d}",
                "province_name": province["name"],
                "region": province["region"],
                "capital_city": province["capital"],
                "population": population,
                "gdp": gdp,
                "area": area,
                "is_municipality": province["is_municipality"],
                "is_special_administrative_region": province["is_special"],
                "create_time": generate_timestamp("2022-01-01", "2022-06-30"),
                "update_time": generate_timestamp("2022-07-01", "2022-12-31")
            })
    
    # 如果不足50条，添加虚构的国际地区
    international_regions = ["北美", "南美", "欧洲", "非洲", "大洋洲", "东南亚", "中东"]
    international_cities = [
        "纽约", "洛杉矶", "多伦多", "伦敦", "巴黎", "柏林", "罗马", 
        "悉尼", "东京", "首尔", "新加坡", "迪拜", "莫斯科", "圣保罗", 
        "开普敦", "墨西哥城"
    ]
    
    while len(data) < DIM_RECORDS:
        i = len(data) + 1
        region = random.choice(international_regions)
        city = random.choice(international_cities)
        international_cities.remove(city)  # 确保城市不重复
        
        data.append({
            "province_id": f"INTL{i:02d}",
            "province_name": f"{city}地区",
            "region": region,
            "capital_city": city,
            "population": random.randint(100, 2000) * 10000,
            "gdp": round(random.uniform(500, 50000), 2),
            "area": round(random.uniform(500, 100000), 2),
            "is_municipality": False,
            "is_special_administrative_region": False,
            "create_time": generate_timestamp("2022-01-01", "2022-06-30"),
            "update_time": generate_timestamp("2022-07-01", "2022-12-31")
        })
    
    # 转换为DataFrame并保存
    df = pd.DataFrame(data)
    df.to_csv(f"{OUTPUT_DIR}/dim_user_province.csv", index=False, sep='\t')
    print(f"已生成{len(df)}条用户省份维度表数据")
    return df

def main():
    """主函数"""
    print("开始生成用户省份维度表数据...")
    
    # 生成维度表数据
    dim_user_province_df = generate_dim_user_province()
    
    print(f"用户省份维度表数据已生成完成，保存在{OUTPUT_DIR}目录下")

if __name__ == "__main__":
    main()