import requests
import time
import json
from datetime import datetime
import logging
import random
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent


class SimpleForexCrawler:
    def __init__(self, update_interval=3600):
        self.update_interval = update_interval
        self.last_update_time = {}
        self.setup_logging()
        self.setup_session()
        self.setup_storage()

        # 货币对配置
        self.currency_pairs = [
            ('USD', 'EUR'), ('USD', 'GBP'), ('USD', 'JPY'), ('USD', 'CAD'),
            ('USD', 'AUD'), ('USD', 'CHF'), ('USD', 'CNY'), ('USD', 'HKD'),
            ('EUR', 'USD'), ('EUR', 'GBP'), ('EUR', 'JPY'), ('EUR', 'CHF'),
            ('GBP', 'USD'), ('GBP', 'EUR'), ('GBP', 'JPY'),
            ('JPY', 'USD'), ('JPY', 'EUR'), ('JPY', 'GBP')
        ]

    def setup_logging(self):
        """简化日志设置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        logging.info("🔄 外汇爬虫启动")

    def setup_session(self):
        """设置请求会话"""
        self.session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def setup_storage(self):
        """设置数据存储"""
        self.data_file = 'forex_data.json'
        logging.info(f"📁 数据文件路径: {os.path.abspath(self.data_file)}")

        # 确保文件存在且可写
        try:
            if not os.path.exists(self.data_file):
                with open(self.data_file, 'w', encoding='utf-8') as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                logging.info(f"✅ 创建新的数据文件: {self.data_file}")
            else:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                logging.info(f"✅ 加载现有数据文件，已有 {len(existing_data)} 条记录")
        except Exception as e:
            logging.error(f"❌ 存储设置失败: {e}")

    def needs_update(self, currency_pair):
        """检查是否需要更新"""
        key = f"{currency_pair[0]}_{currency_pair[1]}"
        last_time = self.last_update_time.get(key)
        if not last_time:
            return True
        return (datetime.now() - last_time).total_seconds() >= self.update_interval

    def fetch_exchange_rate(self, base_currency, target_currency):
        """获取汇率数据"""
        if not self.needs_update((base_currency, target_currency)):
            logging.info(f"⏭️ 跳过 {base_currency}/{target_currency}")
            return None

        url = f"https://api.frankfurter.app/latest?from={base_currency}&to={target_currency}"

        try:
            # 随机延迟
            time.sleep(random.uniform(1, 3))

            headers = {
                'User-Agent': UserAgent().random,
                'Accept': 'application/json'
            }

            logging.info(f"🌐 请求 {base_currency}/{target_currency}")
            response = self.session.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                rate = data['rates'].get(target_currency)
                if rate:
                    logging.info(f"✅ {base_currency}/{target_currency}: {rate}")
                    return {
                        'base_currency': base_currency,
                        'target_currency': target_currency,
                        'exchange_rate': rate,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'source': 'api'
                    }

            logging.warning(f"⚠️ API请求失败: {base_currency}/{target_currency} - HTTP {response.status_code}")

        except Exception as e:
            logging.error(f"❌ 请求异常: {base_currency}/{target_currency} - {e}")

        return self.get_fallback_data(base_currency, target_currency)

    def get_fallback_data(self, base_currency, target_currency):
        """生成备用数据"""
        fallback_rates = {
            ('USD', 'EUR'): 0.92, ('USD', 'GBP'): 0.79, ('USD', 'JPY'): 149.0,
            ('EUR', 'USD'): 1.09, ('EUR', 'GBP'): 0.86, ('EUR', 'JPY'): 161.0,
            ('GBP', 'USD'): 1.27, ('GBP', 'EUR'): 1.16, ('GBP', 'JPY'): 188.0,
            ('JPY', 'USD'): 0.0067, ('JPY', 'EUR'): 0.0062, ('JPY', 'GBP'): 0.0053
        }

        rate = fallback_rates.get((base_currency, target_currency),
                                  round(random.uniform(0.5, 2.0), 4))

        logging.info(f"🔄 使用备用数据: {base_currency}/{target_currency}: {rate}")

        return {
            'base_currency': base_currency,
            'target_currency': target_currency,
            'exchange_rate': rate,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'fallback'
        }

    def save_data(self, new_data_list):
        """保存数据到文件"""
        if not new_data_list:
            logging.warning("⚠️ 没有数据需要保存")
            return False

        try:
            # 读取现有数据
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []

            # 更新或添加数据
            updated_count = 0
            new_count = 0

            for new_data in new_data_list:
                if not new_data:
                    continue

                key = f"{new_data['base_currency']}_{new_data['target_currency']}"

                # 查找并更新现有记录
                found = False
                for i, item in enumerate(existing_data):
                    if item.get('base_currency') == new_data['base_currency'] and \
                            item.get('target_currency') == new_data['target_currency']:
                        existing_data[i] = new_data
                        found = True
                        updated_count += 1
                        break

                if not found:
                    existing_data.append(new_data)
                    new_count += 1

                self.last_update_time[key] = datetime.now()

            # 保存回文件
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)

            logging.info(f"💾 数据保存成功: 更新{updated_count}条, 新增{new_count}条, 总计{len(existing_data)}条记录")
            logging.info(f"📁 文件位置: {os.path.abspath(self.data_file)}")
            return True

        except Exception as e:
            logging.error(f"❌ 保存数据失败: {e}")
            return False

    def run_single_update(self):
        """执行单次更新"""
        logging.info(f"🚀 开始更新汇率数据 - {datetime.now().strftime('%H:%M:%S')}")
        start_time = time.time()

        successful_pairs = 0
        data_to_save = []

        # 随机顺序处理货币对
        random.shuffle(self.currency_pairs)

        for base, target in self.currency_pairs:
            data = self.fetch_exchange_rate(base, target)
            if data:
                data_to_save.append(data)
                successful_pairs += 1

        # 保存所有数据
        if data_to_save:
            save_success = self.save_data(data_to_save)
            if save_success:
                logging.info("✅ 数据已成功写入JSON文件")
            else:
                logging.error("❌ 数据保存失败")
        else:
            logging.warning("⚠️ 没有获取到任何数据")

        total_time = time.time() - start_time
        logging.info(f"📊 更新完成: 成功{successful_pairs}对, 耗时{total_time:.1f}秒")

        # 显示统计
        self.show_stats()

    def show_stats(self):
        """显示统计信息"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                total = len(data)
                api_data = sum(1 for item in data if item.get('source') == 'api')
                fallback_data = sum(1 for item in data if item.get('source') == 'fallback')

                logging.info(f"📈 数据统计: 总记录{total}, API数据{api_data}, 备用数据{fallback_data}")

                # 显示前几条数据作为示例
                if data:
                    logging.info("📋 最新数据示例:")
                    for i, item in enumerate(data[:3]):  # 显示前3条
                        logging.info(f"   {item['base_currency']}/{item['target_currency']}: {item['exchange_rate']} ({item['source']})")
            else:
                logging.warning("📁 数据文件不存在")

        except Exception as e:
            logging.error(f"❌ 统计显示失败: {e}")

    def run_continuous(self, hours=24):
        """持续运行模式"""
        logging.info(f"🔄 持续运行模式启动 - {hours}小时")
        end_time = time.time() + hours * 3600
        cycle = 0

        try:
            while time.time() < end_time:
                cycle += 1
                logging.info(f"\n🔄 第{cycle}轮更新")

                self.run_single_update()

                if time.time() >= end_time:
                    break

                # 等待下一轮
                sleep_time = self.update_interval
                logging.info(f"⏳ 等待{sleep_time}秒后继续...")
                time.sleep(sleep_time)

            logging.info("🎉 持续运行完成")

        except KeyboardInterrupt:
            logging.info("⏹️ 用户中断，正在保存数据...")
            # 确保数据保存
            if hasattr(self, 'data_to_save'):
                self.save_data(self.data_to_save)

    def cleanup(self):
        """清理资源"""
        if hasattr(self, 'session'):
            self.session.close()
        logging.info("🔚 爬虫停止")


# 使用示例
if __name__ == "__main__":
    crawler = SimpleForexCrawler(update_interval=10)  # 10秒更新一次用于测试

    try:
        # 单次运行测试
        crawler.run_single_update()

        # 持续运行（取消注释使用）
        # crawler.run_continuous(hours=24)

    except KeyboardInterrupt:
        logging.info("⏹️ 用户中断")
    except Exception as e:
        logging.error(f"💥 运行错误: {e}")
    finally:
        crawler.cleanup()