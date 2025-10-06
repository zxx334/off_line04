import requests
import json
import time
import subprocess
import sys

class QwenGPUDeployment:
    def __init__(self):
        self.base_url = "http://localhost:11434/api"
        self.model = "qwen3:0.6b"

    def check_gpu_availability(self):
        """检查GPU是否可用"""
        try:
            # 检查Ollama是否支持GPU
            response = requests.get(f"{self.base_url}/version")
            version_info = response.json()
            print(f"Ollama版本: {version_info}")

            # 检查NVIDIA GPU
            try:
                result = subprocess.run(['nvidia-smi'],
                                        capture_output=True, text=True)
                if result.returncode == 0:
                    print("✅ NVIDIA GPU 可用")
                    return True
            except:
                print("❌ 未检测到NVIDIA GPU")
                return False

        except Exception as e:
            print(f"GPU检查失败: {e}")
            return False

    def generate_with_gpu(self, prompt):
        """GPU优化调用"""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_ctx": 4096,  # GPU可以支持更大的上下文
                "temperature": 0.7,
                "num_gpu": 1,     # 使用GPU层
                "num_thread": 8   # 优化线程数
            }
        }

        try:
            response = requests.post(f"{self.base_url}/generate",
                                     json=payload, timeout=30)
            result = response.json()
            return result.get('response', '生成失败')
        except Exception as e:
            return f"GPU部署调用失败: {str(e)}"

# 使用示例
if __name__ == "__main__":
    gpu_deploy = QwenGPUDeployment()

    print("=== Qwen3-0.6B GPU 部署测试 ===")
    gpu_available = gpu_deploy.check_gpu_availability()

    if not gpu_available:
        print("⚠️  GPU不可用，将回退到CPU模式")

    while True:
        user_input = input("\n请输入问题 (输入 'quit' 退出): ")
        if user_input.lower() == 'quit':
            break

        start_time = time.time()
        response = gpu_deploy.generate_with_gpu(user_input)
        end_time = time.time()

        print(f"\n回答: {response}")
        print(f"响应时间: {end_time - start_time:.2f}秒")
        print(f"部署模式: {'GPU' if gpu_available else 'CPU'}")