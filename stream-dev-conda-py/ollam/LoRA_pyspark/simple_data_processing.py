# simple_data_processing.py
import pandas as pd
import json
import re
import os
from typing import List, Dict

class SimpleAddressDataProcessor:
    def __init__(self):
        pass

    def load_raw_data(self, file_path: str) -> List[Dict]:
        """加载原始地址数据"""
        print("正在加载原始数据...")

        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        elif file_path.endswith('.csv'):
            data = pd.read_csv(file_path).to_dict('records')
        else:
            # 如果没有数据文件，使用内置示例
            data = [
                {"address": "北京市海淀区中关村大街1号"},
                {"address": "上海市浦东新区张江高科技园区"},
                {"address": "广东省深圳市南山区科技园南路100号"}
            ]
            print("⚠️ 使用内置示例数据")

        print(f"原始数据量: {len(data)} 条")
        return data

    def clean_address_data(self, data: List[Dict], address_column: str = "address") -> List[Dict]:
        """清洗地址数据"""
        print("正在清洗地址数据...")

        cleaned_data = []
        for item in data:
            address = item.get(address_column, '')
            if not address:
                continue

            # 去除特殊字符和多余空格
            cleaned = re.sub(r'[^\w\u4e00-\u9fff\s\-\.]', '', str(address))
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()

            if len(cleaned) >= 3:  # 过滤过短地址
                item['cleaned_address'] = cleaned
                cleaned_data.append(item)

        print(f"清洗后数据量: {len(cleaned_data)} 条")
        return cleaned_data

    def extract_address_components(self, data: List[Dict]) -> List[Dict]:
        """提取地址组件"""
        print("正在提取地址组件...")

        for item in data:
            address = item.get('cleaned_address', '')
            components = {
                "province": "",
                "city": "",
                "district": "",
                "street": "",
                "detail": address
            }

            # 地址解析规则
            patterns = {
                'province': r'([^省]+省)',
                'city': r'([^市]+市)',
                'district': r'([^区]+区|[^县]+县)',
                'street': r'([^街]+街|[^路]+路|[^道]+道)'
            }

            for comp, pattern in patterns.items():
                match = re.search(pattern, address)
                if match:
                    components[comp] = match.group(1)
                    components['detail'] = components['detail'].replace(match.group(1), '')

            item['address_components'] = components

        return data

    def generate_training_pairs(self, data: List[Dict]) -> List[Dict]:
        """生成训练数据对"""
        print("正在生成训练数据对...")

        training_data = []

        for item in data:
            components = item['address_components']
            base_address = f"{components.get('province', '')}{components.get('city', '')}{components.get('district', '')}{components.get('street', '')}{components.get('detail', '')}".strip()

            # 生成多种问答对
            pairs = [
                {
                    "instruction": f"这个地址在哪个省？{base_address}",
                    "output": f"这个地址在{components['province']}" if components.get('province') else "无法确定省份"
                },
                {
                    "instruction": f"这个地址在哪个市？{base_address}",
                    "output": f"这个地址在{components['city']}" if components.get('city') else "无法确定城市"
                },
                {
                    "instruction": f"请标准化这个地址：{base_address}",
                    "output": f"标准化地址：{components.get('province', '')}{components.get('city', '')}{components.get('district', '')}{components.get('street', '')}{components.get('detail', '')}".strip()
                }
            ]

            for pair in pairs:
                if "无法确定" not in pair["output"]:  # 只添加有效的数据
                    training_data.append({
                        "instruction": pair["instruction"],
                        "input": "",
                        "output": pair["output"],
                        "original_address": base_address
                    })

        # 去重
        unique_data = []
        seen = set()
        for item in training_data:
            key = (item['instruction'], item['output'])
            if key not in seen:
                seen.add(key)
                unique_data.append(item)

        print(f"生成的训练数据量: {len(unique_data)} 条")
        return unique_data

    def save_training_data(self, data: List[Dict], output_path: str):
        """保存训练数据"""
        print("正在保存训练数据...")

        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"训练数据已保存到: {output_path}")

    def process_pipeline(self, input_path: str, output_path: str):
        """完整的数据处理流水线"""
        print("=== 开始地址数据处理流水线 ===")

        # 1. 加载数据
        raw_data = self.load_raw_data(input_path)

        # 2. 清洗数据
        cleaned_data = self.clean_address_data(raw_data)

        # 3. 解析地址组件
        parsed_data = self.extract_address_components(cleaned_data)

        # 4. 生成训练数据
        training_data = self.generate_training_pairs(parsed_data)

        # 5. 保存训练数据
        self.save_training_data(training_data, output_path)

        print("=== 数据处理完成 ===")
        return training_data

# 使用示例
if __name__ == "__main__":
    processor = SimpleAddressDataProcessor()

    # 运行数据处理流水线
    training_data = processor.process_pipeline(
        input_path="sample_address_data.json",
        output_path="training_data/qwen_address_training.json"
    )

    print(f"生成了 {len(training_data)} 条训练样本")