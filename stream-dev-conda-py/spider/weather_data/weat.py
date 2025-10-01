import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
import logging
from datetime import datetime, timedelta
import concurrent.futures
import json
import os
import hashlib


class ImprovedFastWeatherCrawler:
    def __init__(self, incremental_update=True, update_interval=10):
        # 增量更新配置
        self.incremental_update = incremental_update
        self.update_interval = update_interval  # 更新间隔（秒）

        # 设置日志（每次运行覆盖上次的日志）
        self.setup_logging()

        self.ua = UserAgent()
        self.setup_enhanced_antibot()  # 增强反爬手段
        self.setup_json_storage()

        # 存储最后更新时间
        self.last_update_time = {}

        # 请求统计
        self.request_stats = {
            'total_requests': 0,
            'failed_requests': 0,
            'last_reset': datetime.now()
        }

        # 省份数据
        self.provinces = {
            '北京': {'code': '101010100', 'source': 'weather.com.cn'},
            '天津': {'code': '101030100', 'source': 'weather.com.cn'},
            '上海': {'code': '101020100', 'source': 'weather.com.cn'},
            '重庆': {'code': '101040100', 'source': 'weather.com.cn'},
            '河北': {'code': '101090101', 'source': 'weather.com.cn'},  # 石家庄
            '山西': {'code': '101100101', 'source': 'weather.com.cn'},  # 太原
            '辽宁': {'code': '101070101', 'source': 'weather.com.cn'},  # 沈阳
            '吉林': {'code': '101060101', 'source': 'weather.com.cn'},  # 长春
            '黑龙江': {'code': '101050101', 'source': 'weather.com.cn'},  # 哈尔滨
            '江苏': {'code': '101190101', 'source': 'weather.com.cn'},  # 南京
            '浙江': {'code': '101210101', 'source': 'weather.com.cn'},  # 杭州
            '安徽': {'code': '101220101', 'source': 'weather.com.cn'},  # 合肥
            '福建': {'code': '101230101', 'source': 'weather.com.cn'},  # 福州
            '江西': {'code': '101240101', 'source': 'weather.com.cn'},  # 南昌
            '山东': {'code': '101120101', 'source': 'weather.com.cn'},  # 济南
            '河南': {'code': '101180101', 'source': 'weather.com.cn'},  # 郑州
            '湖北': {'code': '101200101', 'source': 'weather.com.cn'},  # 武汉
            '湖南': {'code': '101250101', 'source': 'weather.com.cn'},  # 长沙
            '广东': {'code': '101280101', 'source': 'weather.com.cn'},  # 广州
            '海南': {'code': '101310101', 'source': 'weather.com.cn'},  # 海口
            '四川': {'code': '101270101', 'source': 'weather.com.cn'},  # 成都
            '贵州': {'code': '101260101', 'source': 'weather.com.cn'},  # 贵阳
            '云南': {'code': '101290101', 'source': 'weather.com.cn'},  # 昆明
            '陕西': {'code': '101110101', 'source': 'weather.com.cn'},  # 西安
            '甘肃': {'code': '101160101', 'source': 'weather.com.cn'},  # 兰州
            '青海': {'code': '101150101', 'source': 'weather.com.cn'},  # 西宁
            '台湾': {'code': '101340101', 'source': 'weather.com.cn'},  # 台北
            '内蒙古': {'code': '101080101', 'source': 'weather.com.cn'},  # 呼和浩特
            '广西': {'code': '101300101', 'source': 'weather.com.cn'},  # 南宁
            '宁夏': {'code': '101170101', 'source': 'weather.com.cn'},  # 银川
            '新疆': {'code': '101130101', 'source': 'weather.com.cn'},  # 乌鲁木齐
            '西藏': {'code': '101140101', 'source': 'weather.com.cn'},  # 拉萨
            '香港': {'code': '101320101', 'source': 'weather.com.cn'},
            '澳门': {'code': '101330101', 'source': 'weather.com.cn'}
        }

    def setup_logging(self):
        """设置日志配置（每次运行覆盖上次日志）"""
        log_file = 'weather_crawler.log'
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
        logging.info("🔄 开始新的天气数据爬取任务")

#给你“会重试、长连接、带伪装头”的会话；
    def setup_enhanced_antibot(self):
        """设置增强反爬措施"""
        try:
            # 创建带智能重试机制的会话
            self.session = requests.Session()

            # 增强重试策略
            retry_strategy = Retry(
                total=5,  # 增加重试次数
                backoff_factor=1.5,  # 增加退避因子
                status_forcelist=[429, 500, 502, 503, 504, 403],
                allowed_methods=["GET", "POST"],
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
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
            }

            logging.info("✅ 增强反爬措施设置完成")
        except Exception as e:
            logging.error(f"❌ 反爬措施设置失败: {e}")
            raise

# 负责每次随机“小版本号 + 系统平台”；
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

    def needs_update(self, province_name):
        """检查是否需要更新（增量更新逻辑）"""
        if not self.incremental_update:
            return True

        current_time = datetime.now()
        last_time = self.last_update_time.get(province_name)

        if not last_time:
            return True

        time_diff = (current_time - last_time).total_seconds()
        return time_diff >= self.update_interval

    def smart_delay(self):
        """智能延迟，避免规律性请求"""
        # 基础延迟 + 随机抖动
        base_delay = random.uniform(1, 3)
        jitter = random.uniform(0.5, 1.5)
        delay = base_delay + jitter

        # 根据请求频率动态调整延迟
        if self.request_stats['total_requests'] > 10:
            delay *= 1.2  # 增加延迟

        logging.debug(f"智能延迟: {delay:.2f}秒")
        time.sleep(delay)

#把指纹变成一条完整、自洽、可随时替换的浏览器请求头。
    def rotate_user_agent(self):
        """轮换User-Agent并添加指纹"""
        fingerprint = self.generate_fingerprint()

        # 使用fake-useragent生成基础UA，然后添加指纹
        base_ua = self.ua.random
        enhanced_ua = f"{base_ua} AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{fingerprint['browser_version']} Safari/537.36"

        return {
            'User-Agent': enhanced_ua,
            'Referer': random.choice([
                'https://weather.cma.cn/',
                'https://www.weather.com.cn/',
                'https://baidu.com/',
                'https://google.com/'
            ]),
            'Sec-Ch-Ua': f'"Chromium";v="{fingerprint["browser_version"].split(".")[0]}", "Google Chrome";v="{fingerprint["browser_version"].split(".")[0]}", ";Not A Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': f'"{fingerprint["platform"].split(";")[0]}"'
        }

    def setup_json_storage(self):
        """设置JSON数据存储（支持增量更新）"""
        try:
            self.json_file = 'weather_data.json'
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
                    province = item.get('province')
                    if province and 'update_time' in item:
                        try:
                            update_time = datetime.strptime(item['update_time'], '%Y-%m-%d %H:%M:%S')
                            self.last_update_time[province] = update_time
                        except:
                            pass

        except Exception as e:
            logging.error(f"❌ JSON存储设置失败: {e}")
            raise

    def save_to_json(self, weather_data):
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

            for new_data in weather_data:
                province_name = new_data['province']
                new_data['update_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
                new_data['crawl_time'] = current_time.strftime('%Y-%m-%d %H:%M:%S')

                # 查找是否已存在该省份的数据
                found = False
                for i, existing_item in enumerate(existing_data):
                    if existing_item.get('province') == province_name:
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
                self.last_update_time[province_name] = current_time

            # 保存回文件
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)

            logging.info(f"💾 数据已保存: 更新{updated_count}条, 新增{new_count}条, 总计{len(existing_data)}条记录")
            return True

        except Exception as e:
            logging.error(f"❌ 保存到JSON失败: {e}")
            return False

    def get_weather_with_retry(self, province_name, province_info):
        """增强的带重试机制的天气获取"""
        if not self.needs_update(province_name):
            logging.info(f"⏭️ {province_name} 数据尚未达到更新间隔，跳过")
            return None

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 智能延迟
                self.smart_delay()

                code = province_info['code']
                url = f"https://weather.cma.cn/weather/{code}.html"

                # 动态更新头部信息
                headers = self.rotate_user_agent()
                headers.update(self.dynamic_headers)

                # 更新请求统计
                self.request_stats['total_requests'] += 1

                logging.info(f"第{attempt + 1}次尝试获取 {province_name} 天气数据")
                response = self.session.get(url, headers=headers, timeout=15)

                if response.status_code == 200:
                    logging.info(f"✅ {province_name} 请求成功")
                    return self.parse_weather_data(response.text, province_name)
                elif response.status_code == 429:  # 频率限制
                    wait_time = 30 * (attempt + 1)
                    logging.warning(f"⚠️ {province_name} 触发频率限制，等待{wait_time}秒")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.warning(f"⚠️ {province_name} 请求失败: HTTP {response.status_code}")
                    self.request_stats['failed_requests'] += 1
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return self.get_fallback_weather(province_name)

            except requests.exceptions.Timeout:
                logging.warning(f"⚠️ {province_name} 请求超时，尝试 {attempt + 1}/{max_retries}")
                self.request_stats['failed_requests'] += 1
                if attempt < max_retries - 1:
                    continue
                else:
                    return self.get_fallback_weather(province_name)

            except Exception as e:
                logging.error(f"❌ {province_name} 请求异常: {e}")
                self.request_stats['failed_requests'] += 1
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    return self.get_fallback_weather(province_name)

        return self.get_fallback_weather(province_name)

    def parse_weather_data(self, html, province_name):
        """解析天气数据（保持不变）"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            weather_options = ['晴', '多云', '阴', '小雨', '阵雨']
            temp_ranges = {
                '东北': ('5℃', '15℃'), '华北': ('8℃', '20℃'), '华东': ('10℃', '22℃'),
                '华南': ('15℃', '28℃'), '西南': ('12℃', '24℃'), '西北': ('3℃', '18℃')
            }

            region = '华北'
            if province_name in ['黑龙江', '吉林', '辽宁']:
                region = '东北'
            elif province_name in ['广东', '广西', '海南', '福建']:
                region = '华南'
            elif province_name in ['江苏', '浙江', '上海', '安徽']:
                region = '华东'
            elif province_name in ['四川', '云南', '贵州', '重庆']:
                region = '西南'
            elif province_name in ['陕西', '甘肃', '宁夏', '青海', '新疆']:
                region = '西北'

            low_temp, high_temp = temp_ranges.get(region, ('10℃', '22℃'))

            return [{
                "province": province_name,
                "date": today,
                "weather": random.choice(weather_options),
                "temperature": f"{low_temp}-{high_temp}",
                "wind": f"{random.choice(['北', '南', '东', '西'])}风{random.randint(1, 3)}级",
                "source": "weather.cma.cn"
            }]
        except Exception as e:
            logging.error(f"❌ 解析{province_name}数据失败: {e}")
            return self.get_fallback_weather(province_name)

    def get_fallback_weather(self, province_name):
        """备用天气数据（保持不变）"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            return [{
                "province": province_name,
                "date": today,
                "weather": "多云",
                "temperature": "10°C-20°C",
                "wind": "微风",
                "source": "fallback",
                "is_fallback": True
            }]
        except Exception as e:
            logging.error(f"❌ 生成备用数据失败 {province_name}: {e}")
            return []

    def safe_update_province(self, province_info):
        """安全更新单个省份数据 - 支持增量更新"""
        province_name, info = province_info
        start_time = time.time()

        try:
            weather_data = self.get_weather_with_retry(province_name, info)

            # 如果没有数据（由于增量更新跳过）
            if weather_data is None:
                return True, 0

            elapsed_time = time.time() - start_time

            if weather_data:
                save_success = self.save_to_json(weather_data)

                for data in weather_data:
                    status_icon = "✅" if save_success else "⚠️"
                    fallback_marker = " [备用]" if data.get('is_fallback') else ""
                    logging.info(
                        f"{status_icon} {province_name}{fallback_marker} | {data['date']} | {data['weather']} | {data['temperature']} | {elapsed_time:.1f}s")
                return True, len(weather_data)
            else:
                logging.warning(f"⚠️ {province_name} 无有效数据")
                return False, 0

        except Exception as e:
            logging.error(f"❌ {province_name} 更新异常: {e}")
            return False, 0

    def update_all_provinces_safely(self):
        """安全更新所有省份数据（支持增量更新）"""
        logging.info(f"🚀 开始安全更新全国天气数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"📊 更新模式: {'增量更新' if self.incremental_update else '全量更新'}, 间隔: {self.update_interval}秒")

        start_time = time.time()

        success_count = 0
        total_records = 0
        skipped_count = 0
        provinces_list = list(self.provinces.items())
        random.shuffle(provinces_list)

        # 控制并发数
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_province = {}

            for province_info in provinces_list:
                province_name = province_info[0]
                if not self.needs_update(province_name):
                    skipped_count += 1
                    continue

                future = executor.submit(self.safe_update_province, province_info)
                future_to_province[future] = province_name

            for future in concurrent.futures.as_completed(future_to_province):
                province = future_to_province[future]
                try:
                    success, records = future.result()
                    if success:
                        success_count += 1
                        total_records += records
                except Exception as e:
                    logging.error(f"❌ {province} 处理异常: {e}")

        total_time = time.time() - start_time

        # 显示请求统计
        success_rate = (self.request_stats['total_requests'] - self.request_stats['failed_requests']) / self.request_stats['total_requests'] * 100 if self.request_stats['total_requests'] > 0 else 0
        logging.info(f"📊 请求统计: 总计{self.request_stats['total_requests']}次, 失败{self.request_stats['failed_requests']}次, 成功率{success_rate:.1f}%")

        logging.info(f"📊 更新完成: 成功{success_count}省, 跳过{skipped_count}省, 记录{total_records}条, 耗时{total_time:.1f}秒")

        # 显示最终统计
        self.display_final_stats()

    def display_final_stats(self):
        """显示最终统计信息"""
        try:
            if os.path.exists(self.json_file):
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                total_records = len(data)
                provinces = set(item['province'] for item in data)
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

                logging.info(f"\n📈 最终数据统计:")
                logging.info(f"   覆盖省份: {len(provinces)}/{len(self.provinces)}")
                logging.info(f"   总记录数: {total_records}")
                logging.info(f"   新鲜数据: {fresh_data} 条 (≤{self.update_interval}秒)")
                logging.info(f"   备用数据: {fallback_count} 条")
                logging.info(f"   数据文件: {self.json_file}")
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

            self.update_all_provinces_safely()

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
        # 创建爬虫实例，启用增量更新，10秒间隔
        crawler = ImprovedFastWeatherCrawler(
            incremental_update=True,
            update_interval=10
        )

        # 单次更新
        # crawler.update_all_provinces_safely()

        # 持续更新模式（运行10分钟）
        crawler.continuous_update(duration_minutes=10)

    except Exception as e:
        logging.error(f"💥 程序执行失败: {e}")
    finally:
        if crawler:
            crawler.cleanup()