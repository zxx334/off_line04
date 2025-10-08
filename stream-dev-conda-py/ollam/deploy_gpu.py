import requests
import json
import time
import subprocess
import sys
import psutil

class QwenGPUDeployment:
    def __init__(self):
        self.base_url = "http://localhost:11434/api"
        self.model = "qwen3:0.6b"

    def check_gpu_availability(self):
        """检查GPU是否可用并返回详细信息"""
        gpu_info = {
            "available": False,
            "gpu_memory_total": 0,
            "gpu_memory_used": 0,
            "gpu_memory_free": 0,
            "gpu_name": "Unknown"
        }

        try:
            # 检查Ollama是否支持GPU
            response = requests.get(f"{self.base_url}/version")
            version_info = response.json()
            print(f"Ollama版本: {version_info}")

            # 检查NVIDIA GPU
            try:
                result = subprocess.run(['nvidia-smi',
                                         '--query-gpu=name,memory.total,memory.used,memory.free',
                                         '--format=csv,noheader,nounits'],
                                        capture_output=True, text=True, encoding='utf-8')

                if result.returncode == 0:
                    gpu_data = result.stdout.strip().split(', ')
                    if len(gpu_data) >= 4:
                        gpu_info.update({
                            "available": True,
                            "gpu_name": gpu_data[0],
                            "gpu_memory_total": int(gpu_data[1]),
                            "gpu_memory_used": int(gpu_data[2]),
                            "gpu_memory_free": int(gpu_data[3])
                        })

                    print("✅ NVIDIA GPU 可用")
                    print(f"GPU型号: {gpu_info['gpu_name']}")
                    print(f"显存总量: {gpu_info['gpu_memory_total']} MB")
                    print(f"已用显存: {gpu_info['gpu_memory_used']} MB")
                    print(f"剩余显存: {gpu_info['gpu_memory_free']} MB")

            except Exception as e:
                print(f"❌ GPU检测失败: {e}")
                gpu_info["available"] = False

        except Exception as e:
            print(f"Ollama连接失败: {e}")

        return gpu_info

    def get_system_memory_usage(self):
        """获取系统内存使用情况"""
        memory = psutil.virtual_memory()
        return {
            "total_memory": memory.total // (1024 * 1024),  # MB
            "used_memory": memory.used // (1024 * 1024),    # MB
            "available_memory": memory.available // (1024 * 1024),  # MB
            "memory_percent": memory.percent
        }

    def get_ollama_memory_usage(self):
        """获取Ollama进程内存使用情况"""
        try:
            for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
                if 'ollama' in proc.info['name'].lower():
                    memory_mb = proc.info['memory_info'].rss // (1024 * 1024)
                    return {
                        "ollama_memory_mb": memory_mb,
                        "ollama_pid": proc.info['pid']
                    }
        except Exception as e:
            print(f"获取Ollama内存使用失败: {e}")

        return {"ollama_memory_mb": 0, "ollama_pid": None}

    def monitor_gpu_memory_real_time(self):
        """实时监控GPU内存使用情况"""
        try:
            result = subprocess.run(['nvidia-smi',
                                     '--query-gpu=memory.used,memory.total,utilization.gpu',
                                     '--format=csv,noheader,nounits'],
                                    capture_output=True, text=True, encoding='utf-8')

            if result.returncode == 0:
                gpu_data = result.stdout.strip().split(', ')
                if len(gpu_data) >= 3:
                    used_memory = int(gpu_data[0])
                    total_memory = int(gpu_data[1])
                    gpu_utilization = int(gpu_data[2])

                    return {
                        "gpu_used_mb": used_memory,
                        "gpu_total_mb": total_memory,
                        "gpu_utilization_percent": gpu_utilization,
                        "gpu_usage_percent": (used_memory / total_memory) * 100
                    }
        except Exception as e:
            print(f"GPU实时监控失败: {e}")

        return None

    def generate_with_gpu(self, prompt):
        """GPU优化调用，包含内存监控"""
        # 调用前监控
        pre_gpu_info = self.monitor_gpu_memory_real_time()
        pre_system_memory = self.get_system_memory_usage()
        pre_ollama_memory = self.get_ollama_memory_usage()

        print(f"\n📊 调用前状态:")
        if pre_gpu_info:
            print(f"GPU使用: {pre_gpu_info['gpu_used_mb']}/{pre_gpu_info['gpu_total_mb']} MB ({pre_gpu_info['gpu_usage_percent']:.1f}%)")
        print(f"系统内存: {pre_system_memory['used_memory']}/{pre_system_memory['total_memory']} MB ({pre_system_memory['memory_percent']:.1f}%)")
        print(f"Ollama内存: {pre_ollama_memory['ollama_memory_mb']} MB")

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_ctx": 4096,
                "temperature": 0.7,
                "num_gpu": 1,     # 使用GPU层
                "num_thread": 8
            }
        }

        try:
            response = requests.post(f"{self.base_url}/generate",
                                     json=payload, timeout=30)
            result = response.json()

            # 调用后监控
            post_gpu_info = self.monitor_gpu_memory_real_time()
            post_system_memory = self.get_system_memory_usage()
            post_ollama_memory = self.get_ollama_memory_usage()

            print(f"\n📊 调用后状态:")
            if post_gpu_info:
                print(f"GPU使用: {post_gpu_info['gpu_used_mb']}/{post_gpu_info['gpu_total_mb']} MB ({post_gpu_info['gpu_usage_percent']:.1f}%)")
                if pre_gpu_info:
                    gpu_increase = post_gpu_info['gpu_used_mb'] - pre_gpu_info['gpu_used_mb']
                    print(f"GPU内存增加: {gpu_increase} MB")

            ollama_increase = post_ollama_memory['ollama_memory_mb'] - pre_ollama_memory['ollama_memory_mb']
            print(f"Ollama内存增加: {ollama_increase} MB")

            return result.get('response', '生成失败')

        except Exception as e:
            return f"GPU部署调用失败: {str(e)}"

# 使用示例
if __name__ == "__main__":
    gpu_deploy = QwenGPUDeployment()

    print("=== Qwen3-0.6B GPU 部署测试 ===")
    gpu_info = gpu_deploy.check_gpu_availability()

    if not gpu_info["available"]:
        print("⚠️  GPU不可用，将回退到CPU模式")

    while True:
        user_input = input("\n请输入问题 (输入 'quit' 退出): ")
        if user_input.lower() == 'quit':
            break

        start_time = time.time()
        response = gpu_deploy.generate_with_gpu(user_input)
        end_time = time.time()

        print(f"\n🤖 回答: {response}")
        print(f"⏱️  响应时间: {end_time - start_time:.2f}秒")
        print(f"🚀 部署模式: {'GPU' if gpu_info['available'] else 'CPU'}")