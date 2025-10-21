import akshare as ak
import redis
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import requests
import json

class StockDataUpdater:
    def __init__(self, redis_host='106.14.164.40', redis_port=6378, redis_db=3, password="0x2F746bC70f72aAF3340B8BbFd254fd91a3996218"):

        self.redis = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            password=password, decode_responses=True
        )
        self._test_connection()
    
    def _test_connection(self):
        try:
            self.redis.ping()
            print("✅ Redis连接成功")
        except Exception as e:
            print(f"❌ Redis连接失败: {e}")
            raise
    
    def get_all_stock_codes(self):
        """从Redis获取所有股票代码"""
        try:
            codes = self.redis.smembers("stocks:all_codes")
            print(f"📊 从Redis获取到 {len(codes)} 只股票代码")
            return list(codes)
        except Exception as e:
            print(f"❌ 获取股票代码失败: {e}")
            return []
    
    def safe_float_convert(self, value, default=0.0):
        """安全转换为float - 修复版"""
        try:
            if value is None or value == '':
                return default
            
            if isinstance(value, str) and value.lower() in ['nan', 'null', 'none', '']:
                return default
            
            value_float = float(value)
            
            if np.isnan(value_float) or np.isinf(value_float):
                return default
                
            return value_float
            
        except (ValueError, TypeError) as e:
            print(f"数值转换失败: {value} -> {e}")
            return default
    
    def get_stock_real_time_data(self, stock_code):
        """获取单只股票的实时数据 - 原有逻辑不变"""
        try:
            time.sleep(random.uniform(0.1, 0.3))
            
            if stock_code.startswith('6'):
                full_code = f"sh{stock_code}"
            else:
                full_code = f"sz{stock_code}"
            
            url = f"http://qt.gtimg.cn/q={full_code}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200 and response.text:
                data = response.text.split('~')
                if len(data) > 40:
                    current_price = self.safe_float_convert(data[3])
                    pe_ratio = self.safe_float_convert(data[39])
                    
                    return {
                        'price': current_price,
                        'pe': pe_ratio,
                        'success': True
                    }
            
            return {'success': False, 'error': '接口返回数据异常'}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_esg_score_data(self, stock_code):
        """获取ESG评分数据 - 新增方法"""
        try:
            # 添加延迟，避免请求过快
            time.sleep(random.uniform(0.3, 0.5))
            
            # 格式化股票代码
            if stock_code.startswith('6'):
                formatted_code = f"{stock_code}.SH"
            else:
                formatted_code = f"{stock_code}.SZ"
            
            url = "https://www.chindices.com/esg-carbon/api/esg/total_score"
            params = {
                'stockCode': formatted_code,
                'flag': 'ESG_A'
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
                'Referer': 'https://www.chindices.com/',
                'Accept': 'application/json, text/plain, */*'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('code') == 200 and data.get('msg') == 'SUCCESS':
                    esg_data = data.get('data', {})
                    
                    return {
                        'success': True,
                        'esg_score': self.safe_float_convert(esg_data.get('score', 0)),
                        'esg_grade': esg_data.get('totalScore', 'N/A'),
                        'esg_ranking': esg_data.get('ranking', 'N/A'),
                        'esg_color': esg_data.get('color', 'N/A'),
                        'esg_date': esg_data.get('date', 'N/A'),
                        'esg_industry': esg_data.get('industryClass', 'N/A')
                    }
                else:
                    return {
                        'success': False,
                        'error': f"API返回错误: {data.get('msg', '未知错误')}"
                    }
            else:
                return {
                    'success': False,
                    'error': f"HTTP错误: {response.status_code}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def update_single_stock(self, stock_code):
        """更新单只股票数据 - 原有逻辑完全不变"""
        try:
            # 获取实时数据 - 完全保持原有逻辑
            real_time_data = self.get_stock_real_time_data(stock_code)
            
            if not real_time_data['success']:
                print(f"❌ {stock_code} 获取实时数据失败: {real_time_data.get('error')}")
                return False
            
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取股票名称（从Redis中）
            stock_info = self.redis.hgetall(f"stock:hash:{stock_code}")
            stock_name = stock_info.get('name', '未知')
            
            # 更新Hash数据 - 原有字段完全不变
            update_data = {
                'price': str(real_time_data['price']),
                'pe': str(real_time_data['pe']),
                'update_time': update_time,
                'last_sync': update_time
            }
            self.redis.hset(f"stock:hash:{stock_code}", mapping=update_data)
            
            # 更新价格排序集合
            if real_time_data['price'] > 0:
                self.redis.zadd("stocks:by_price", {stock_code: real_time_data['price']})
            
            # 更新PE排序集合
            if real_time_data['pe'] > 0 and real_time_data['pe'] < 10000:
                self.redis.zadd("stocks:by_pe", {stock_code: real_time_data['pe']})
            
            print(f"✅ {stock_code} {stock_name} 更新成功: 价格{real_time_data['price']}, PE{real_time_data['pe']}")
            return True
            
        except Exception as e:
            print(f"❌ {stock_code} 更新失败: {e}")
            return False
    
    def update_esg_data_only(self, stock_code):
        """单独更新ESG数据 - 新增方法"""
        try:
            # 获取ESG数据
            esg_data = self.get_esg_score_data(stock_code)
            
            if not esg_data['success']:
                print(f"❌ {stock_code} 获取ESG数据失败: {esg_data.get('error')}")
                return False
            
            # 获取股票名称
            stock_info = self.redis.hgetall(f"stock:hash:{stock_code}")
            stock_name = stock_info.get('name', '未知')
            
            # 更新ESG数据到Redis
            esg_update_data = {
                'esg_score': str(esg_data['esg_score']),
                'esg_grade': esg_data['esg_grade'],
                'esg_ranking': esg_data['esg_ranking'],
                'esg_color': esg_data['esg_color'],
                'esg_date': esg_data['esg_date'],
                'esg_industry': esg_data['esg_industry'],
                'esg_update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 将ESG数据存储到单独的Hash中，避免影响原有数据结构
            self.redis.hset(f"stock:esg:{stock_code}", mapping=esg_update_data)
            
            # 添加到ESG评分排序集合
            if esg_data['esg_score'] > 0:
                self.redis.zadd("stocks:by_esg_score", {stock_code: esg_data['esg_score']})
            
            print(f"✅ {stock_code} {stock_name} ESG数据更新成功: 评分{esg_data['esg_score']}, 等级{esg_data['esg_grade']}, 排名{esg_data['esg_ranking']}")
            return True
            
        except Exception as e:
            print(f"❌ {stock_code} ESG数据更新失败: {e}")
            return False
    
    def batch_update_stocks(self, batch_size=50, delay_between_batches=3):
        """批量更新股票数据 - 原有逻辑完全不变"""
        all_codes = self.get_all_stock_codes()
        if not all_codes:
            print("❌ 没有找到股票代码，无法更新")
            return
        
        total_count = len(all_codes)
        success_count = 0
        fail_count = 0
        
        print(f"🚀 开始批量更新 {total_count} 只股票数据...")
        print(f"📦 批次大小: {batch_size}, 批次间隔: {delay_between_batches}秒")
        
        for i in range(0, total_count, batch_size):
            batch_codes = all_codes[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_count + batch_size - 1) // batch_size
            
            print(f"\n🔄 正在处理第 {batch_num}/{total_batches} 批次 ({len(batch_codes)}只股票)...")
            
            batch_success = 0
            batch_fail = 0
            
            for stock_code in batch_codes:
                if self.update_single_stock(stock_code):  # 只更新原有数据
                    batch_success += 1
                    success_count += 1
                else:
                    batch_fail += 1
                    fail_count += 1
            
            print(f"📊 第{batch_num}批次完成: 成功{batch_success}, 失败{batch_fail}")
            
            if i + batch_size < total_count:
                print(f"⏳ 等待{delay_between_batches}秒后继续下一批次...")
                time.sleep(delay_between_batches)
        
        # 更新元数据
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.redis.set("stocks:last_update", update_time)
        self.redis.set("stocks:last_batch_update", update_time)
        
        print(f"\n🎉 批量更新完成！")
        print(f"📈 统计结果:")
        print(f"   总处理: {total_count} 只")
        print(f"   成功: {success_count} 只")
        print(f"   失败: {fail_count} 只")
        print(f"   成功率: {success_count/total_count*100:.2f}%")
        print(f"   更新时间: {update_time}")
    
    def batch_update_esg_data(self, batch_size=30, delay_between_batches=5):
        """批量更新ESG数据 - 新增方法"""
        all_codes = self.get_all_stock_codes()
        if not all_codes:
            print("❌ 没有找到股票代码，无法更新ESG数据")
            return
        
        total_count = len(all_codes)
        success_count = 0
        fail_count = 0
        
        print(f"🚀 开始批量更新 {total_count} 只股票的ESG数据...")
        print(f"📦 批次大小: {batch_size}, 批次间隔: {delay_between_batches}秒")
        
        for i in range(0, total_count, batch_size):
            batch_codes = all_codes[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_count + batch_size - 1) // batch_size
            
            print(f"\n🔄 正在处理第 {batch_num}/{total_batches} 批次 ESG数据 ({len(batch_codes)}只股票)...")
            
            batch_success = 0
            batch_fail = 0
            
            for stock_code in batch_codes:
                if self.update_esg_data_only(stock_code):
                    batch_success += 1
                    success_count += 1
                else:
                    batch_fail += 1
                    fail_count += 1
            
            print(f"📊 第{batch_num}批次ESG数据完成: 成功{batch_success}, 失败{batch_fail}")
            
            if i + batch_size < total_count:
                print(f"⏳ 等待{delay_between_batches}秒后继续下一批次...")
                time.sleep(delay_between_batches)
        
        # 更新ESG元数据
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.redis.set("stocks:esg_last_update", update_time)
        self.redis.set("stocks:esg_total_count", success_count)
        
        print(f"\n🎉 ESG数据批量更新完成！")
        print(f"📈 ESG统计结果:")
        print(f"   总处理: {total_count} 只")
        print(f"   成功: {success_count} 只")
        print(f"   失败: {fail_count} 只")
        print(f"   成功率: {success_count/total_count*100:.2f}%")
        print(f"   更新时间: {update_time}")
    
    def show_current_stats(self):
        """显示当前数据统计 - 增强版，包含ESG数据"""
        print(f"\n📊 当前Redis数据统计:")
        total_stocks = self.redis.scard("stocks:all_codes")
        last_update = self.redis.get("stocks:last_update") or "从未更新"
        esg_last_update = self.redis.get("stocks:esg_last_update") or "从未更新"
        
        print(f"股票总数: {total_stocks}")
        print(f"最后价格更新: {last_update}")
        print(f"最后ESG更新: {esg_last_update}")
        print(f"价格排序集合: {self.redis.zcard('stocks:by_price')}")
        print(f"PE排序集合: {self.redis.zcard('stocks:by_pe')}")
        print(f"ESG评分集合: {self.redis.zcard('stocks:by_esg_score')}")
        print(f"ESG数据数量: {len(self.redis.keys('stock:esg:*'))}")
        
        # 显示几只股票的ESG数据示例
        print(f"\n🔍 ESG数据示例:")
        sample_codes = list(self.redis.smembers("stocks:all_codes"))[:2]
        for code in sample_codes:
            esg_data = self.redis.hgetall(f"stock:esg:{code}")
            if esg_data:
                name = self.redis.hget(f"stock:hash:{code}", 'name') or '未知'
                score = esg_data.get('esg_score', '未知')
                grade = esg_data.get('esg_grade', '未知')
                ranking = esg_data.get('esg_ranking', '未知')
                print(f"  {code} {name}: ESG评分{score}, 等级{grade}, 排名{ranking}")

# 使用示例
def main():
    updater = StockDataUpdater()
    
    print("请选择更新模式:")
    print("1. 更新股票价格数据（原有功能）")
    print("2. 更新ESG评分数据（新增功能）")
    print("3. 同时更新价格和ESG数据")
    
    choice = input("请输入选择 (1/2/3): ").strip()
    
    if choice == "1":
        updater.batch_update_stocks()
    elif choice == "2":
        updater.batch_update_esg_data()
    elif choice == "3":
        print("🔄 先更新股票价格数据...")
        updater.batch_update_stocks()
        print("\n🔄 再更新ESG数据...")
        updater.batch_update_esg_data()
    else:
        print("❌ 无效选择")
    
    # 显示最终统计
    updater.show_current_stats()

if __name__ == "__main__":
    main()