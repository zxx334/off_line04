import time
import requests

def compare_performance():
    """对比CPU和GPU性能"""
    test_prompts = [
        "请介绍人工智能的发展历史",
        "写一个Python函数计算斐波那契数列",
        "解释一下机器学习中的过拟合现象"
    ]

    base_url = "http://localhost:11434/api"
    model = "qwen3:0.6b"

    print("=== CPU vs GPU 性能对比 ===\n")

    for i, prompt in enumerate(test_prompts, 1):
        print(f"测试 {i}: {prompt}")

        # CPU测试
        cpu_payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_gpu": 0}  # 强制使用CPU
        }

        # GPU测试
        gpu_payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"num_gpu": 1}  # 使用GPU
        }

        # 执行测试
        start_time = time.time()
        cpu_response = requests.post(f"{base_url}/generate", json=cpu_payload)
        cpu_time = time.time() - start_time

        start_time = time.time()
        gpu_response = requests.post(f"{base_url}/generate", json=gpu_payload)
        gpu_time = time.time() - start_time

        print(f"  CPU时间: {cpu_time:.2f}秒")
        print(f"  GPU时间: {gpu_time:.2f}秒")
        print(f"  性能提升: {cpu_time/gpu_time:.1f}x\n")

if __name__ == "__main__":
    compare_performance()