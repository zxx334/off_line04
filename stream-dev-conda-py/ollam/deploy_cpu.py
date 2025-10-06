import requests
import json
import time

class QwenCPUDeployment:
    def __init__(self):
        self.base_url = "http://localhost:11434/api"
        self.model = "qwen3:0.6b"

    def generate(self, prompt):
        """CPU模式调用"""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_ctx": 2048,  # 上下文长度
                "temperature": 0.7
            }
        }

        try:
            response = requests.post(f"{self.base_url}/generate",
                                     json=payload, timeout=60)
            result = response.json()
            return result.get('response', '生成失败')
        except Exception as e:
            return f"CPU部署调用失败: {str(e)}"

    def get_system_info(self):
        """获取系统信息"""
        try:
            response = requests.get("http://localhost:11434/api/tags")
            return response.json()
        except:
            return "无法连接到Ollama服务"

# 使用示例
if __name__ == "__main__":
    cpu_deploy = QwenCPUDeployment()

    print("=== Qwen3-0.6B CPU 部署测试 ===")
    print("系统信息:", cpu_deploy.get_system_info())

    while True:
        user_input = input("\n请输入问题 (输入 'quit' 退出): ")
        if user_input.lower() == 'quit':
            break

        start_time = time.time()
        response = cpu_deploy.generate(user_input)
        end_time = time.time()

        print(f"\n回答: {response}")
        print(f"响应时间: {end_time - start_time:.2f}秒")