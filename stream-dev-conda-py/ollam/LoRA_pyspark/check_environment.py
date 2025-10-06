# check_environment.py
import subprocess
import sys
import os

def check_java():
    """检查 Java 环境"""
    print("=== 检查 Java 环境 ===")
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java 已安装")
            print(result.stderr)
        else:
            print("❌ Java 未安装或配置错误")
    except Exception as e:
        print(f"❌ 无法执行 java 命令: {e}")

def check_pyspark():
    """检查 PySpark 环境"""
    print("\n=== 检查 PySpark 环境 ===")
    try:
        import pyspark
        print(f"✅ PySpark 已安装: {pyspark.__version__}")

        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("EnvironmentCheck") \
            .getOrCreate()

        print(f"✅ Spark 版本: {spark.version}")
        print(f"✅ Spark Master: {spark.sparkContext.master}")

        spark.stop()
        return True
    except ImportError:
        print("❌ PySpark 未安装")
        return False
    except Exception as e:
        print(f"❌ Spark 环境错误: {e}")
        return False

if __name__ == "__main__":
    check_java()
    check_pyspark()