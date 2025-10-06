import requests
import json
import time

# 配置
API_BASE = "http://localhost:8000/api"
CALLBACK_BASE = "http://localhost:9000"

def test_sync_chat():
    """测试同步聊天"""
    print("=== 测试同步聊天 ===")

    payload = {
        "prompt": "请用三句话介绍深度学习",
        "temperature": 0.7
    }

    try:
        response = requests.post(f"{API_BASE}/chat/sync", json=payload)
        result = response.json()

        print(f"状态: {response.status_code}")
        print(f"响应: {result.get('response')}")
        print(f"处理时间: {result.get('processing_time')}")

    except Exception as e:
        print(f"同步聊天测试失败: {e}")

def test_async_chat():
    """测试异步聊天（带回调）"""
    print("\n=== 测试异步聊天（带回调）===")

    payload = {
        "prompt": "解释一下神经网络的基本原理",
        "callback_url": f"{CALLBACK_BASE}/callback/receive",
        "task_id": f"test_async_{int(time.time())}"
    }

    try:
        response = requests.post(f"{API_BASE}/chat/async", json=payload)
        result = response.json()

        print(f"任务ID: {result.get('task_id')}")
        print(f"状态: {result.get('status')}")
        print(f"消息: {result.get('message')}")
        print("请查看回调接收器的控制台输出...")

    except Exception as e:
        print(f"异步聊天测试失败: {e}")

def test_callback_registration():
    """测试回调注册"""
    print("\n=== 测试回调注册 ===")

    payload = {
        "callback_url": f"{CALLBACK_BASE}/callback/receive"
    }

    try:
        response = requests.post(f"{API_BASE}/callback/register", json=payload)
        result = response.json()

        print(f"消息: {result.get('message')}")
        print(f"回调URL: {result.get('callback_url')}")
        print(f"总处理器数: {result.get('total_handlers')}")

    except Exception as e:
        print(f"回调注册测试失败: {e}")

def test_health_checks():
    """测试健康检查"""
    print("\n=== 测试健康检查 ===")

    try:
        # 测试API服务健康状态
        api_health = requests.get(f"{API_BASE}/health")
        print(f"API服务状态: {api_health.json().get('status')}")

        # 测试回调接收器健康状态
        callback_health = requests.get(f"{CALLBACK_BASE}/health")
        print(f"回调接收器状态: {callback_health.json().get('status')}")

    except Exception as e:
        print(f"健康检查失败: {e}")

if __name__ == '__main__':
    print("开始测试 Qwen 云端部署服务...")

    # 运行测试
    test_health_checks()
    test_sync_chat()
    test_callback_registration()
    test_async_chat()

    print("\n=== 测试完成 ===")