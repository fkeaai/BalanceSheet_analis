import pandas as pd
import redis
import numpy as np
from datetime import datetime
import os

class CSVToRedisImporter:
    # def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, password=None):
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

    def load_csv_data(self):
        """加载CSV数据并清理NaN值"""
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("未找到CSV文件")
        
        csv_file = csv_files[0]
        print(f"📁 读取CSV文件: {csv_file}")
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
        except:
            df = pd.read_csv(csv_file, encoding='gbk')
        
        # 清理数据：处理NaN值
        df = self.clean_data(df)
        print(f"✅ 成功读取 {len(df)} 行数据")
        return df

    def clean_data(self, df):
        """清理数据中的NaN值"""
        # 处理价格列的NaN
        if '最新价格' in df.columns:
            df['最新价格'] = df['最新价格'].fillna(0)
            df['最新价格'] = df['最新价格'].replace([np.nan, np.inf, -np.inf], 0)
        
        # 处理PE列的NaN
        if '市盈率(PE)' in df.columns:
            df['市盈率(PE)'] = df['市盈率(PE)'].fillna(0)
            df['市盈率(PE)'] = df['市盈率(PE)'].replace([np.nan, np.inf, -np.inf], 0)
        
        return df

    def safe_float_convert(self, value, default=0.0):
        """安全转换为float，处理NaN和异常值"""
        try:
            if pd.isna(value) or value is None:
                return default
            value = float(value)
            if np.isnan(value) or np.isinf(value):
                return default
            return value
        except (ValueError, TypeError):
            return default

    def import_to_redis(self):
        """导入数据到Redis - 修复NaN问题"""
        df = self.load_csv_data()
        
        # 清空现有数据
        self.clear_redis_data()
        
        print("🚀 开始导入数据到Redis...")
        
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        pipe = self.redis.pipeline()
        success_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                # 安全获取数据
                stock_code = str(row['股票代码']).zfill(6)
                stock_name = row['股票名称']
                
                # 安全转换数值
                price = self.safe_float_convert(row['最新价格'], 0.0)
                pe = self.safe_float_convert(row['市盈率(PE)'], 0.0)
                
                print(f"处理 {stock_code}: 价格={price}, PE={pe}")
                
                # 数据结构1: Hash存储详细信息
                stock_info = {
                    'name': stock_name,
                    'price': str(price),
                    'pe': str(pe),
                    'update_time': update_time
                }
                pipe.hset(f"stock:hash:{stock_code}", mapping=stock_info)
                
                # 数据结构2: 有序集合 - 按价格排序（只添加有效价格）
                if price > 0:
                    pipe.zadd("stocks:by_price", {stock_code: price})
                
                # 数据结构3: 有序集合 - 按PE排序（只添加有效PE）
                if pe > 0 and pe < 10000:  # 限制PE范围，排除异常值
                    pipe.zadd("stocks:by_pe", {stock_code: pe})
                elif pe <= 0:  # 负PE或0PE单独处理
                    pipe.zadd("stocks:by_pe_invalid", {stock_code: pe})
                
                # 数据结构4: 集合 - 所有股票代码
                pipe.sadd("stocks:all_codes", stock_code)
                
                success_count += 1
                
                # 每50条执行一次，避免管道过大
                if success_count % 50 == 0:
                    pipe.execute()
                    print(f"✅ 已处理 {success_count} 条数据...")
                    pipe = self.redis.pipeline()
                    
            except Exception as e:
                error_count += 1
                print(f"❌ 处理第{index+1}行失败: {e}")
                # 重置管道，避免错误累积
                try:
                    pipe.execute()
                except:
                    pass
                pipe = self.redis.pipeline()
                continue
        
        # 执行剩余的管道命令
        try:
            pipe.execute()
        except Exception as e:
            print(f"执行最后批量操作时出错: {e}")
        
        # 保存元数据
        self.redis.set("stocks:last_update", update_time)
        self.redis.set("stocks:total_count", success_count)
        
        print(f"\n🎉 导入完成！")
        print(f"✅ 成功: {success_count} 条")
        print(f"❌ 失败: {error_count} 条")
        self.show_stats()

    def clear_redis_data(self):
        """清空Redis中的股票数据"""
        patterns = [
            "stock:hash:*",
            "stocks:by_price",
            "stocks:by_pe",
            "stocks:by_pe_invalid",
            "stocks:all_codes",
            "stocks:last_update",
            "stocks:total_count"
        ]
        
        deleted_count = 0
        for pattern in patterns:
            if '*' in pattern:
                keys = self.redis.keys(pattern)
                if keys:
                    count = self.redis.delete(*keys)
                    deleted_count += count
            else:
                if self.redis.exists(pattern):
                    self.redis.delete(pattern)
                    deleted_count += 1
        
        print(f"🗑️ 已清空 {deleted_count} 个数据键")

    def show_stats(self):
        """显示统计信息"""
        print(f"\n📊 Redis数据结构统计:")
        print(f"Hash存储: {len(self.redis.keys('stock:hash:*'))}")
        print(f"价格排序集合: {self.redis.zcard('stocks:by_price')}")
        print(f"有效PE排序集合: {self.redis.zcard('stocks:by_pe')}")
        print(f"无效PE集合: {self.redis.zcard('stocks:by_pe_invalid')}")
        print(f"总股票代码数: {self.redis.scard('stocks:all_codes')}")
        print(f"最后更新: {self.redis.get('stocks:last_update')}")

# 运行导入
if __name__ == "__main__":
    importer = CSVToRedisImporter()
    importer.import_to_redis()

