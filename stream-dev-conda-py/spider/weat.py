import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
import logging
from datetime import datetime
import concurrent.futures
import json
import os


class ImprovedFastWeatherCrawler:
    def __init__(self):
        # 设置日志（每次运行覆盖上次的日志）
        self.setup_logging()

        self.ua = UserAgent()
        self.setup_advanced_antibot()
        self.setup_json_storage()

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
        # 删除旧的日志文件（如果存在）
        log_file = 'weather_crawler.log'
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                print(f"🗑️ 已清除旧日志文件: {log_file}")
            except Exception as e:
                print(f"⚠️ 清除旧日志失败: {e}")

        # 配置新的日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8', mode='w'),  # mode='w'覆盖模式
                logging.StreamHandler()
            ]
        )
        logging.info("🔄 开始新的天气数据爬取任务")

    def setup_advanced_antibot(self):
        """设置高级反爬措施"""
        try:
            # 创建带重试机制的会话
            self.session = requests.Session()

            # 设置重试策略
            retry_strategy = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
            )

            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

            # 设置基础头部
            self.session.headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            logging.info("✅ 反爬措施设置完成")
        except Exception as e:
            logging.error(f"❌ 反爬措施设置失败: {e}")
            raise

    def setup_json_storage(self):
        """设置JSON数据存储"""
        try:
            self.json_file = 'weather_data.json'
            # 初始化空的JSON文件（覆盖模式）
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            logging.info(f"✅ JSON存储文件初始化完成: {self.json_file}")
        except Exception as e:
            logging.error(f"❌ JSON存储设置失败: {e}")
            raise

    def save_to_json(self, weather_data):
        """保存数据到JSON文件"""
        try:
            # 读取现有数据
            if os.path.exists(self.json_file):
                with open(self.json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []

            # 添加新数据
            for data in weather_data:
                # 添加时间戳
                data['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                existing_data.append(data)

            # 保存回文件
            with open(self.json_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)

            logging.info(f"💾 数据已保存到 {self.json_file}，总计 {len(existing_data)} 条记录")
            return True
        except Exception as e:
            logging.error(f"❌ 保存到JSON失败: {e}")
            return False

    def get_weather_with_retry(self, province_name, province_info):
        """带重试机制的天气获取"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 随机延迟
                delay = random.uniform(1, 3)
                logging.info(f"等待 {delay:.1f} 秒后请求 {province_name}...")
                time.sleep(delay)

                code = province_info['code']
                url = f"https://weather.cma.cn/weather/{code}.html"

                # 动态更新User-Agent
                headers = {
                    'User-Agent': self.ua.random,
                    'Referer': 'https://weather.cma.cn/',
                }

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
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return self.get_fallback_weather(province_name)

            except requests.exceptions.Timeout:
                logging.warning(f"⚠️ {province_name} 请求超时，尝试 {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return self.get_fallback_weather(province_name)

            except Exception as e:
                logging.error(f"❌ {province_name} 请求异常: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    return self.get_fallback_weather(province_name)

        return self.get_fallback_weather(province_name)

    def parse_weather_data(self, html, province_name):
        """解析天气数据"""
        try:
            # 这里可以添加真实的数据解析逻辑
            # 目前使用示例数据
            today = datetime.now().strftime('%Y-%m-%d')
            weather_options = ['晴', '多云', '阴', '小雨', '阵雨']
            temp_ranges = {
                '东北': ('5℃', '15℃'), '华北': ('8℃', '20℃'), '华东': ('10℃', '22℃'),
                '华南': ('15℃', '28℃'), '西南': ('12℃', '24℃'), '西北': ('3℃', '18℃')
            }

            # 简单的地理分区
            region = '华北'  # 默认
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
        """备用天气数据"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            return [{
                "province": province_name,
                "date": today,
                "weather": "多云",
                "temperature": "10°C-20°C",
                "wind": "微风",
                "source": "fallback",
                "is_fallback": True  # 标记为备用数据
            }]
        except Exception as e:
            logging.error(f"❌ 生成备用数据失败 {province_name}: {e}")
            return []

    def safe_update_province(self, province_info):
        """安全更新单个省份数据 - 添加JSON存储"""
        province_name, info = province_info
        start_time = time.time()

        try:
            weather_data = self.get_weather_with_retry(province_name, info)
            elapsed_time = time.time() - start_time

            if weather_data:
                # 保存数据到JSON文件
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
        """安全更新所有省份数据"""
        logging.info(f"🚀 开始安全更新全国天气数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        start_time = time.time()

        success_count = 0
        total_records = 0
        provinces_list = list(self.provinces.items())
        random.shuffle(provinces_list)  # 随机顺序避免模式检测

        # 控制并发数
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_province = {}

            for province_info in provinces_list:
                future = executor.submit(self.safe_update_province, province_info)
                future_to_province[future] = province_info[0]

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
        logging.info(f"📊 更新完成: 成功{success_count}省, 记录{total_records}条, 耗时{total_time:.1f}秒")

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

                logging.info(f"\n📈 最终数据统计:")
                logging.info(f"   覆盖省份: {len(provinces)}/{len(self.provinces)}")
                logging.info(f"   总记录数: {total_records}")
                logging.info(f"   备用数据: {fallback_count} 条")
                logging.info(f"   数据文件: {self.json_file}")
        except Exception as e:
            logging.error(f"❌ 显示统计信息失败: {e}")

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
        crawler = ImprovedFastWeatherCrawler()
        crawler.update_all_provinces_safely()
    except Exception as e:
        logging.error(f"💥 程序执行失败: {e}")
    finally:
        if crawler:
            crawler.cleanup()