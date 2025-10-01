import requests
import time
import json
from datetime import datetime
import logging
import concurrent.futures
import random
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent


class ImprovedForexCrawler:
    def __init__(self, incremental_update=True, update_interval=3600):
        """
        增强版外汇数据爬虫
        :param incremental_update: 是否启用增量更新
        :param update_interval: 更新间隔时间(秒)
        """
        # 增量更新配置
        self.incremental_update = incremental_update
        self.update_interval = update_interval

        # 设置日志（每次运行覆盖上次的日志）
        self.setup_logging()

        self.ua = UserAgent()
        self.setup_enhanced_antibot()
        self.setup_json_storage()

        # 存储最后更新时间
        self.last_update_time = {}

        # 请求统计
        self.request_stats = {
            'total_requests': 0,
            'failed_requests': 0,
            'last_reset': datetime.now()
        }

        # 支持的货币对
        self.currency_pairs = {
            'USD': ['EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'HKD'],
            'EUR': ['USD', 'GBP', 'JPY', 'CHF'],
            'GBP': ['USD', 'EUR', 'JPY'],
            'JPY': ['USD', 'EUR', 'GBP']
        }

        # 增量数据存储
        self.incremental_file = 'forex_incremental_data.json'
        self.setup_incremental_storage()

        logging.info("✅ 外汇数据爬虫初始化完成")

    def setup_logging(self):
        """设置日志配置（每次运行覆盖上次日志）"""
        log_file = 'forex_crawler.log'
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                print(f"🗑️ 已清除旧日志文件: {log_file}")
            except Exception as e:
                print(f"⚠️ 清除旧日志失败: {e}")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8', mode='w'),
                logging.StreamHandler()
            ]
        )
        logging.info("🔄 开始新的外汇数据爬取任务")

    def setup_enhanced_antibot(self):
        """设置增强反爬措施"""
        try:
            # 创建带智能重试机制的会话
            self.session = requests.Session()

            # 增强重试策略
            retry_strategy = Retry(
                total=5,
                backoff_factor=1.5,
                status_forcelist=[429, 500, 502, 503, 504, 403],
                allowed_methods=["GET"],
                respect_retry_after_header=True
            )

            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=10
            )
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

            # 动态头部信息
            self.dynamic_headers = {
                'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }

            logging.info("✅ 增强反爬措施设置完成")
        except Exception as e:
            logging.error(f"❌ 反爬措施设置失败: {e}")
            raise

    def setup_json_storage(self):
        """设置JSON数据存储（支持增量更新）"""
        try:
            self.json_file = 'forex_data.json'
            # 如果文件不存在，初始化空列表
            if not os.path.exists(self.json_file):
                with open(self.json_file, 'w', encoding='utf-8') as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                logging.info(f"✅ JSON存储文件初始化完成: {self.json_file}")
            else:
                logging.info(f"✅ 使用现有JSON存储文件: {self.json_file}")

                # 加载现有数据，初始化最后更新时间
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)

                for item in existing_data:
                    currency_pair = f"{item.get('base_currency')}_{item.get('target_currency')}"
                    if currency_pair and 'update_time' in item:
                        try:
                            update_time = datetime.strptime(item['update_time'], '%Y-%m-%d %H:%M:%S')
                            self.last_update_time[currency_pair] = update_time
                        except:
                            pass

        except Exception as e:
            logging.error(f"❌ JSON存储设置失败: {e}")
            raise

    def setup_incremental_storage(self):
        """设置增量数据存储"""
        try:
            # 初始化增量数据文件（如果不存在）
            if not os.path.exists(self.incremental_file):
                with open(self.incremental_file, 'w', encoding='utf-8') as f:
                    f.write('')  # 创建空文件
                logging.info(f"✅ 增量数据文件初始化完成: {self.incremental_file}")
            else:
                logging.info(f"✅ 使用现有增量数据文件: {self.incremental_file}")

        except Exception as e:
            logging.error(f"❌ 增量数据存储设置失败: {e}")
            raise

    def generate_fingerprint(self):
        """生成浏览器指纹，用于避免检测"""
        browser_versions = [
            '96.0.4664.110', '97.0.4692.71', '98.0.4758.102',
            '99.0.4844.51', '100.0.4896.127', '101.0.4951.67'
        ]
        platform_versions = [
            'Windows NT 10.0; Win64; x64',
            'Windows NT 6.1; Win64; x64',
            'Macintosh; Intel Mac OS X 10_15_7',
            'X11; Linux x86_64'
        ]

        return {
            'browser_version': random.choice(browser_versions),
            'platform': random.choice(platform_versions)
        }

    def rotate_user_agent(self):
        """轮换User-Agent并添加指纹"""
        fingerprint = self.generate_fingerprint()

        base_ua = self.ua.random
        enhanced_ua = f"{base_ua} AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{fingerprint['browser_version']} Safari/537.36"

        return {
            'User-Agent': enhanced_ua,
            'Referer': random.choice([
                'https://api.frankfurter.app/',
                'https://www.xe.com/',
                'https://www.google.com/finance',
                'https://finance.yahoo.com/'
            ])
        }

    def smart_delay(self):
        """智能延迟，避免规律性请求"""
        base_delay = random.uniform(2, 5)
        jitter = random.uniform(0.5, 2.0)
        delay = base_delay + jitter

        # 根据请求频率动态调整延迟
        if self.request_stats['total_requests'] > 5:
            delay *= 1.3

        logging.debug(f"智能延迟: {delay:.2f}秒")
        time.sleep(delay)

    def needs_update(self, currency_pair):
        """检查是否需要更新（增量更新逻辑）"""
        if not self.incremental_update:
            return True

        current_time = datetime.now()
        last_time = self.last_update_time.get(currency_pair)

        if not last_time:
            return True

        time_diff = (current_time - last_time).total_seconds()
        return time_diff >= self.update_interval

    def fetch_currency_data_with_retry(self, base_currency, target_currency):
        """带重试机制的货币数据获取"""
        if not self.needs_update(f"{base_currency}_{target_currency}"):
            logging.info(f"⏭️ {base_currency}/{target_currency} 数据尚未达到更新间隔，跳过")
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 智能延迟
                self.smart_delay()

                url = f"https://api.frankfurter.app/latest?from={base_currency}&to={target_currency}"

                # 动态更新头部信息
                headers = self.rotate_user_agent()
                headers.update(self.dynamic_headers)

                # 更新请求统计
                self.request_stats['total_requests'] += 1

                logging.info(f"第{attempt + 1}次尝试获取 {base_currency}/{target_currency} 汇率数据")
                response = self.session.get(url, headers=headers, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    logging.info(f"✅ {base_currency}/{target_currency} 请求成功，汇率: {data.get('rates', {}).get(target_currency, 'N/A')}")

                    # 添加额外信息
                    data['base_currency'] = base_currency
                    data['target_currency'] = target_currency
                    data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    data['date'] = datetime.now().strftime('%Y-%m-%d')

                    return data

                elif response.status_code == 429:  # 频率限制
                    wait_time = 30 * (attempt + 1)
                    logging.warning(f"⚠️ {base_currency}/{target_currency} 触发频率限制，等待{wait_time}秒")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.warning(f"⚠️ {base_currency}/{target_currency} 请求失败: HTTP {response.status_code}")
                    self.request_stats['failed_requests'] += 1
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return self.get_fallback_data(base_currency, target_currency)

            except requests.exceptions.Timeout:
                logging.warning(f"⚠️ {base_currency}/{target_currency} 请求超时，尝试 {attempt + 1}/{max_retries}")
                self.request_stats['failed_requests'] += 1
                if attempt < max_retries - 1:
                    continue
                else:
                    return self.get_fallback_data(base_currency, target_currency)

            except Exception as e:
                logging.error(f"❌ {base_currency}/{target_currency} 请求异常: {e}")
                self.request_stats['failed_requests'] += 1
                if attempt < max_retries - 1:
                    time.sleep(10)
                    continue
                else:
                    return self.get_fallback_data(base_currency, target_currency)

        return self.get_fallback_data(base_currency, target_currency)

    def get_fallback_data(self, base_currency, target_currency):
        """备用汇率数据"""
        try:
            # 生成模拟的备用数据
            base_rates = {
                'USD': {'EUR': 0.85, 'GBP': 0.73, 'JPY': 110.5, 'CAD': 1.25},
                'EUR': {'USD': 1.18, 'GBP': 0.86, 'JPY': 130.0},
                'GBP': {'USD': 1.37, 'EUR': 1.16, 'JPY': 151.0},
                'JPY': {'USD': 0.0091, 'EUR': 0.0077, 'GBP': 0.0066}
            }

            rate = base_rates.get(base_currency, {}).get(target_currency)
            if not rate:
                # 随机生成一个合理的汇率
                rate = round(random.uniform(0.5, 150.0), 4)

            return {
                "base_currency": base_currency,
                "target_currency": target_currency,
                "rates": {target_currency: rate},
                "date": datetime.now().strftime('%Y-%m-%d'),
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "source": "fallback",
                "is_fallback": True
            }
        except Exception as e:
            logging.error(f"❌ 生成备用数据失败 {base_currency}/{target_currency}: {e}")
            return None

    def save_to_json(self, forex_data):
        """增量保存数据到JSON文件"""
        try:
            # 读取现有数据
            if os.path.exists(self.json_file):
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []

            current_time = datetime.now()
            updated_count = 0
            new_count = 0

            for new_data in forex_data:
                if not new_data:
                    continue

                currency_pair = f"{new_data['base_currency']}_{new_data['target_currency']}"
                new_data['update_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                new_data['crawl_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

                # 查找是否已存在该货币对的数据
                found = False
                for i, existing_item in enumerate(existing_data):
                    if (existing_item.get('base_currency') == new_data['base_currency'] and
                            existing_item.get('target_currency') == new_data['target_currency']):
                        # 更新现有数据
                        existing_data[i] = new_data
                        updated_count += 1
                        found = True
                        break

                if not found:
                    # 添加新数据
                    existing_data.append(new_data)
                    new_count += 1

                # 更新最后更新时间
                self.last_update_time[currency_pair] = current_time

            # 保存回文件
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)

            logging.info(f"💾 数据已保存到JSON: 更新{updated_count}条, 新增{new_count}条, 总计{len(existing_data)}条记录")
            return True

        except Exception as e:
            logging.error(f"❌ 保存到JSON失败: {e}")
            return False

    def save_incremental_data(self, forex_data):
        """保存增量数据到单独的文件"""
        try:
            incremental_changes = []
            current_time = datetime.now()

            for data in forex_data:
                if not data:
                    continue

                # 创建增量记录
                incremental_record = {
                    'base_currency': data['base_currency'],
                    'target_currency': data['target_currency'],
                    'exchange_rate': data.get('rates', {}).get(data['target_currency']),
                    'timestamp': data['timestamp'],
                    'update_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'is_fallback': data.get('is_fallback', False)
                }

                incremental_changes.append(incremental_record)

            if incremental_changes:
                # 追加到增量数据文件
                with open(self.incremental_file, 'a', encoding='utf-8') as f:
                    for record in incremental_changes:
                        json.dump(record, f, ensure_ascii=False)
                        f.write('\n')

                logging.info(f"📈 增量数据已保存: {len(incremental_changes)} 条变化")
                return True
            else:
                logging.info("📈 无增量数据变化")
                return False

        except Exception as e:
            logging.error(f"❌ 保存增量数据失败: {e}")
            return False

    def safe_update_currency_pair(self, currency_info):
        """安全更新单个货币对数据"""
        base_currency, target_currency = currency_info
        start_time = time.time()

        try:
            forex_data = self.fetch_currency_data_with_retry(base_currency, target_currency)

            # 如果没有数据（由于增量更新跳过）
            if forex_data is None:
                return True, 0

            elapsed_time = time.time() - start_time

            if forex_data:
                # 转换为列表格式以便统一处理
                data_list = [forex_data] if forex_data else []

                # 保存到主JSON文件
                json_success = self.save_to_json(data_list)

                # 保存增量数据
                incremental_success = self.save_incremental_data(data_list)

                status_icon = "✅" if (json_success or incremental_success) else "⚠️"
                fallback_marker = " [备用]" if forex_data.get('is_fallback') else ""
                rate = forex_data.get('rates', {}).get(target_currency, 'N/A')

                logging.info(
                    f"{status_icon} {base_currency}/{target_currency}{fallback_marker} | "
                    f"汇率: {rate} | 耗时: {elapsed_time:.1f}s"
                )
                return True, 1
            else:
                logging.warning(f"⚠️ {base_currency}/{target_currency} 无有效数据")
                return False, 0

        except Exception as e:
            logging.error(f"❌ {base_currency}/{target_currency} 更新异常: {e}")
            return False, 0

    def update_all_currencies_safely(self):
        """安全更新所有货币对数据"""
        logging.info(f"🚀 开始安全更新外汇汇率数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"📊 更新模式: {'增量更新' if self.incremental_update else '全量更新'}, 间隔: {self.update_interval}秒")

        start_time = time.time()

        success_count = 0
        total_records = 0
        skipped_count = 0

        # 生成所有货币对组合
        currency_pairs = []
        for base_currency, target_currencies in self.currency_pairs.items():
            for target_currency in target_currencies:
                currency_pairs.append((base_currency, target_currency))

        random.shuffle(currency_pairs)  # 随机顺序避免模式检测

        # 控制并发数
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_currency = {}

            for currency_pair in currency_pairs:
                base_currency, target_currency = currency_pair
                pair_key = f"{base_currency}_{target_currency}"

                if not self.needs_update(pair_key):
                    skipped_count += 1
                    continue

                future = executor.submit(self.safe_update_currency_pair, currency_pair)
                future_to_currency[future] = pair_key

            for future in concurrent.futures.as_completed(future_to_currency):
                currency_pair = future_to_currency[future]
                try:
                    success, records = future.result()
                    if success:
                        success_count += 1
                        total_records += records
                except Exception as e:
                    logging.error(f"❌ {currency_pair} 处理异常: {e}")

        total_time = time.time() - start_time

        # 显示请求统计
        success_rate = (self.request_stats['total_requests'] - self.request_stats['failed_requests']) / self.request_stats['total_requests'] * 100 if self.request_stats['total_requests'] > 0 else 0
        logging.info(f"📊 请求统计: 总计{self.request_stats['total_requests']}次, 失败{self.request_stats['failed_requests']}次, 成功率{success_rate:.1f}%")

        logging.info(f"📊 更新完成: 成功{success_count}对, 跳过{skipped_count}对, 记录{total_records}条, 耗时{total_time:.1f}秒")

        # 显示最终统计
        self.display_final_stats()

    def display_final_stats(self):
        """显示最终统计信息"""
        try:
            if os.path.exists(self.json_file):
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                total_records = len(data)
                currency_pairs = set(f"{item.get('base_currency')}_{item.get('target_currency')}" for item in data)
                fallback_count = sum(1 for item in data if item.get('is_fallback'))

                # 计算数据新鲜度
                now = datetime.now()
                fresh_data = 0
                for item in data:
                    if 'update_time' in item:
                        try:
                            update_time = datetime.strptime(item['update_time'], '%Y-%m-%d %H:%M:%S')
                            if (now - update_time).total_seconds() <= self.update_interval:
                                fresh_data += 1
                        except:
                            pass

                # 统计增量数据
                incremental_count = 0
                if os.path.exists(self.incremental_file):
                    with open(self.incremental_file, 'r', encoding='utf-8') as f:
                        incremental_count = sum(1 for line in f if line.strip())

                logging.info(f"\n📈 最终数据统计:")
                logging.info(f"   覆盖货币对: {len(currency_pairs)}")
                logging.info(f"   总记录数: {total_records}")
                logging.info(f"   新鲜数据: {fresh_data} 条 (≤{self.update_interval}秒)")
                logging.info(f"   备用数据: {fallback_count} 条")
                logging.info(f"   增量记录: {incremental_count} 条")
                logging.info(f"   主数据文件: {self.json_file}")
                logging.info(f"   增量数据文件: {self.incremental_file}")
        except Exception as e:
            logging.error(f"❌ 显示统计信息失败: {e}")

    def continuous_update(self, duration_minutes=60):
        """持续更新模式"""
        logging.info(f"🔄 启动持续更新模式，运行{duration_minutes}分钟")
        start_time = time.time()
        end_time = start_time + duration_minutes * 60
        cycle_count = 0

        while time.time() < end_time:
            cycle_count += 1
            logging.info(f"🔄 第{cycle_count}轮更新开始")

            self.update_all_currencies_safely()

            # 计算下一轮更新时间
            next_update = time.time() + self.update_interval
            current_time = time.time()

            if current_time < next_update:
                sleep_time = next_update - current_time
                logging.info(f"⏳ 等待{sleep_time:.1f}秒后进行下一轮更新")
                time.sleep(sleep_time)

        total_duration = (time.time() - start_time) / 60
        logging.info(f"🎉 持续更新完成: 运行{total_duration:.1f}分钟, 完成{cycle_count}轮更新")

    def cleanup(self):
        """清理资源"""
        try:
            if hasattr(self, 'session'):
                self.session.close()
            logging.info("🎉 爬取任务完成，资源已清理")
        except Exception as e:
            logging.error(f"❌ 资源清理失败: {e}")


# 使用示例
if __name__ == "__main__":
    crawler = None
    try:
        # 创建爬虫实例，启用增量更新，1小时间隔
        crawler = ImprovedForexCrawler(
            incremental_update=True,
            update_interval=3600  # 1小时
        )

        # 单次更新
        # crawler.update_all_currencies_safely()

        # 持续更新模式（运行24小时）
        crawler.continuous_update(duration_minutes=1440)  # 24小时

    except Exception as e:
        logging.error(f"💥 程序执行失败: {e}")
    finally:
        if crawler:
            crawler.cleanup()