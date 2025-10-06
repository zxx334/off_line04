# pyspark_data_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import json
import re
import os
import findspark
findspark.init()

class PySparkAddressProcessor:
    def __init__(self):
        """初始化 Spark 会话"""
        print("初始化 Spark 会话...")
        self.spark = SparkSession.builder \
            .appName("AddressDataProcessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.python.worker.reuse", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
            .master("local[1]") \
            .getOrCreate()

        # 正确的缩进
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark 会话初始化完成")

    def create_sample_data(self):
        """创建示例地址数据 - 简化版本避免文件写入问题"""
        print("创建示例地址数据...")

        sample_data = [
            ("北京市海淀区中关村大街1号", "北京", "北京市", "海淀区", "中关村大街"),
            ("上海市浦东新区张江高科技园区", "上海", "上海市", "浦东新区", "张江高科技园区"),
            ("广东省深圳市南山区科技园南路100号", "广东", "深圳市", "南山区", "科技园南路"),
            ("浙江省杭州市西湖区文三路100号", "浙江", "杭州市", "西湖区", "文三路"),
            ("江苏省南京市鼓楼区中山路1号", "江苏", "南京市", "鼓楼区", "中山路"),
            ("四川省成都市武侯区天府软件园", "四川", "成都市", "武侯区", "天府软件园"),
            ("陕西省西安市雁塔区高新路", "陕西", "西安市", "雁塔区", "高新路"),
            ("湖北省武汉市武昌区东湖路", "湖北", "武汉市", "武昌区", "东湖路"),
            ("湖南省长沙市岳麓区橘子洲头", "湖南", "长沙市", "岳麓区", "橘子洲头"),
            ("山东省济南市历下区泉城广场", "山东", "济南市", "历下区", "泉城广场")
        ]

        # 创建 DataFrame
        schema = ["address", "province", "city", "district", "street"]
        df = self.spark.createDataFrame(sample_data, schema)

        print("示例数据创建完成")
        return df

    def clean_address_data(self, df):
        """清洗地址数据"""
        print("清洗地址数据...")

        # 定义清洗UDF
        def clean_address_udf(address):
            if not address:
                return ""
            # 去除特殊字符和多余空格
            cleaned = re.sub(r'[^\w\u4e00-\u9fff\s\-\.]', '', str(address))
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            return cleaned

        clean_udf = udf(clean_address_udf, StringType())

        # 应用清洗
        df_cleaned = df.filter(col("address").isNotNull()) \
            .withColumn("cleaned_address", clean_udf(col("address"))) \
            .filter(length(col("cleaned_address")) >= 3)

        print(f"清洗后数据量: {df_cleaned.count()} 条")
        return df_cleaned

    def extract_address_features(self, df):
        """提取地址特征"""
        print("提取地址特征...")

        # 定义特征提取UDF
        def extract_features_udf(address):
            features = {
                "has_province": 0,
                "has_city": 0,
                "has_district": 0,
                "has_street": 0,
                "address_length": len(address),
                "word_count": len(address.strip())
            }

            # 检查是否包含各级行政区划关键词
            province_keywords = ['省', '自治区']
            city_keywords = ['市', '州']
            district_keywords = ['区', '县', '旗']
            street_keywords = ['街', '路', '道', '巷', '胡同']

            for keyword in province_keywords:
                if keyword in address:
                    features["has_province"] = 1
                    break

            for keyword in city_keywords:
                if keyword in address:
                    features["has_city"] = 1
                    break

            for keyword in district_keywords:
                if keyword in address:
                    features["has_district"] = 1
                    break

            for keyword in street_keywords:
                if keyword in address:
                    features["has_street"] = 1
                    break

            return json.dumps(features)

        features_udf = udf(extract_features_udf, StringType())

        df_with_features = df.withColumn("features", features_udf(col("cleaned_address"))) \
            .withColumn("features_json", from_json(col("features"),
                                                   StructType([
                                                       StructField("has_province", IntegerType()),
                                                       StructField("has_city", IntegerType()),
                                                       StructField("has_district", IntegerType()),
                                                       StructField("has_street", IntegerType()),
                                                       StructField("address_length", IntegerType()),
                                                       StructField("word_count", IntegerType())
                                                   ])))

        # 展开特征列
        for field in ["has_province", "has_city", "has_district", "has_street", "address_length", "word_count"]:
            df_with_features = df_with_features.withColumn(field, col(f"features_json.{field}"))

        return df_with_features.drop("features", "features_json")

    def generate_training_pairs(self, df):
        """生成训练数据对"""
        print("生成训练数据对...")

        # 定义训练数据生成UDF
        def create_training_pairs_udf(address, province, city, district, street):
            pairs = []

            base_address = address

            # 生成省相关问答
            if province and province != "未知":
                pairs.append({
                    "instruction": f"这个地址在哪个省？{base_address}",
                    "output": f"这个地址在{province}省"
                })

            # 生成市相关问答
            if city and city != "未知":
                pairs.append({
                    "instruction": f"这个地址在哪个市？{base_address}",
                    "output": f"这个地址在{city}"
                })

            # 生成区县相关问答
            if district and district != "未知":
                pairs.append({
                    "instruction": f"这个地址在哪个区/县？{base_address}",
                    "output": f"这个地址在{district}"
                })

            # 生成标准化问答
            standardized = f"{province if province and province != '未知' else ''}{city if city and city != '未知' else ''}{district if district and district != '未知' else ''}{street if street and street != '未知' else ''}".strip()
            if standardized:
                pairs.append({
                    "instruction": f"请标准化这个地址：{base_address}",
                    "output": f"标准化地址：{standardized}"
                })

            return json.dumps(pairs)

        training_udf = udf(create_training_pairs_udf, StringType())

        df_with_pairs = df.withColumn("training_pairs", training_udf(
            col("cleaned_address"), col("province"), col("city"), col("district"), col("street")
        ))

        # 展开训练对
        df_exploded = df_with_pairs.withColumn("pair", explode(from_json(col("training_pairs"),
                                                                         ArrayType(StructType([
                                                                             StructField("instruction", StringType()),
                                                                             StructField("output", StringType())
                                                                         ]))
                                                                         )))

        final_df = df_exploded.select(
            col("pair.instruction").alias("instruction"),
            col("pair.output").alias("output"),
            col("cleaned_address").alias("original_address"),
            col("province"),
            col("city"),
            col("district"),
            col("street")
        ).distinct()

        print(f"生成的训练数据量: {final_df.count()} 条")
        return final_df

    def save_training_data(self, df, output_path):
        """保存训练数据"""
        print(f"保存训练数据到: {output_path}")

        # 转换为Pandas DataFrame保存（适合中小数据集）
        pandas_df = df.select("instruction", "output", "original_address").toPandas()

        # 转换为训练格式
        training_data = []
        for _, row in pandas_df.iterrows():
            training_data.append({
                "instruction": row['instruction'],
                "input": "",
                "output": row['output'],
                "original_address": row['original_address']
            })

        # 保存为JSON
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)

        print(f"训练数据保存完成: {len(training_data)} 条")
        return training_data

    def process_pipeline(self, output_path="training_data/pyspark_address_training.json"):
        """完整的数据处理流水线"""
        print("=== 开始 PySpark 地址数据处理流水线 ===")

        try:
            # 1. 创建数据
            print("使用内置示例数据...")
            df = self.create_sample_data()

            # 2. 数据清洗
            df_cleaned = self.clean_address_data(df)

            # 3. 特征提取
            df_features = self.extract_address_features(df_cleaned)

            # 4. 生成训练数据
            df_training = self.generate_training_pairs(df_features)

            # 5. 保存训练数据
            training_data = self.save_training_data(df_training, output_path)

            # 6. 显示样本
            print("\n=== 训练数据样本 ===")
            df_training.select("instruction", "output").show(10, truncate=False)

            print("=== PySpark 数据处理完成 ===")
            return training_data

        except Exception as e:
            print(f"PySpark处理失败: {e}")
            # 回退到简单处理
            return self.fallback_processing(output_path)

    def fallback_processing(self, output_path):
        """PySpark失败时的回退处理"""
        print("使用回退数据处理...")
        sample_data = [
            {"address": "北京市海淀区中关村大街1号", "province": "北京", "city": "北京市", "district": "海淀区", "street": "中关村大街"},
            {"address": "上海市浦东新区张江高科技园区", "province": "上海", "city": "上海市", "district": "浦东新区", "street": "张江高科技园区"},
        ]

        training_data = []
        for item in sample_data:
            training_data.extend([
                {
                    "instruction": f"这个地址在哪个省？{item['address']}",
                    "input": "",
                    "output": f"这个地址在{item['province']}省",
                    "original_address": item['address']
                },
                {
                    "instruction": f"这个地址在哪个市？{item['address']}",
                    "input": "",
                    "output": f"这个地址在{item['city']}",
                    "original_address": item['address']
                }
            ])

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)

        print(f"回退处理完成: {len(training_data)} 条")
        return training_data

    def stop_spark(self):
        """停止 Spark 会话"""
        self.spark.stop()
        print("Spark 会话已停止")

# 使用示例
if __name__ == "__main__":
    processor = PySparkAddressProcessor()

    try:
        # 运行完整流水线
        training_data = processor.process_pipeline(
            output_path="training_data/pyspark_address_training.json"
        )

        print(f"\n🎉 PySpark 数据处理完成！")
        print(f"生成训练样本: {len(training_data)} 条")
        print(f"输出文件: training_data/pyspark_address_training.json")

    except Exception as e:
        print(f"❌ 处理失败: {e}")
        import traceback
        traceback.print_exc()

    finally:
        processor.stop_spark()