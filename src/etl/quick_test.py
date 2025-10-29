"""
快速测试 - 一分钟验证功能
"""

from fetch_sp500_prices_batch import SP500PriceFetcherBatch
import pandas as pd
from pathlib import Path

print("=" * 70)
print("快速测试: 抓取2024年10月的3只股票数据")
print("=" * 70)

# 创建抓取器
fetcher = SP500PriceFetcherBatch(
    start_date="2024-10-01",
    end_date="2024-10-31",
    data_dir="data_test"
)

# 测试3只股票
test_symbols = ['AAPL', 'MSFT', 'GOOGL']

print(f"\n测试股票: {', '.join(test_symbols)}")
print(f"日期范围: 2024-10-01 至 2024-10-31")
print(f"保存位置: data_test/\n")

# 抓取数据
print("开始抓取...")
fetcher.collect_data(test_symbols, delay=0.5)

# 保存数据
print("\n保存数据...")
fetcher.save_all_data()

# 验证数据
print("\n" + "=" * 70)
print("验证结果:")
print("=" * 70)

test_dir = Path("data_test/raw/prices/source=yahoo")
parquet_files = list(test_dir.rglob("*.parquet"))

if parquet_files:
    df = pd.read_parquet(parquet_files[0])
    
    print(f"\n✅ 成功抓取数据!")
    print(f"   文件: {parquet_files[0].name}")
    print(f"   总记录数: {len(df)}")
    print(f"   股票数量: {df['symbol'].nunique()}")
    print(f"   日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
    
    print("\n📊 数据预览 (前5行):")
    print(df.head().to_string())
    
    print("\n" + "=" * 70)
    print("✅ 测试成功!")
    print("=" * 70)
    print("\n下一步:")
    print("  1. 查看测试数据: data_test/raw/prices/source=yahoo/")
    print("  2. 运行完整测试: python test_fetch.py")
    print("  3. 删除测试数据: rm -rf data_test/")
    print("  4. 开始正式抓取: python fetch_sp500_prices_batch.py")
    
else:
    print("\n❌ 未找到数据文件")
    print("请检查日志输出了解原因")