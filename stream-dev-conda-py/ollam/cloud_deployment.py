from flask import Flask, request, jsonify
import requests
import json
import time
import threading
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class QwenCloudDeployment:
    def __init__(self):
        self.ollama_url = "http://localhost:11434/api/generate"
        self.model = "qwen3:0.6b"
        self.callback_handlers = []

    def generate_response(self, prompt, **kwargs):
        """生成模型响应"""
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": kwargs.get('temperature', 0.7),
                "num_ctx": kwargs.get('context_length', 4096),
                "num_gpu": kwargs.get('use_gpu', 1)
            }
        }

        try:
            response = requests.post(self.ollama_url, json=payload, timeout=120)
            response.raise_for_status()
            return response.json().get('response', '')
        except Exception as e:
            logger.error(f"模型调用失败: {e}")
            return f"错误: {str(e)}"

    def register_callback(self, callback_url):
        """注册回调处理器"""
        if callback_url not in self.callback_handlers:
            self.callback_handlers.append(callback_url)
            logger.info(f"注册回调处理器: {callback_url}")

    def notify_callbacks(self, task_id, result):
        """异步通知所有回调处理器"""
        def send_notification(callback_url, data):
            try:
                response = requests.post(
                    callback_url,
                    json=data,
                    headers={'Content-Type': 'application/json'},
                    timeout=30
                )
                logger.info(f"回调通知成功: {callback_url}, 状态: {response.status_code}")
            except Exception as e:
                logger.error(f"回调通知失败 {callback_url}: {e}")

        data = {
            "task_id": task_id,
            "result": result,
            "timestamp": datetime.now().isoformat(),
            "status": "completed"
        }

        for callback_url in self.callback_handlers:
            thread = threading.Thread(
                target=send_notification,
                args=(callback_url, data)
            )
            thread.daemon = True
            thread.start()

# 初始化部署实例
qwen_cloud = QwenCloudDeployment()

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({
        "status": "healthy",
        "service": "Qwen Cloud API",
        "timestamp": datetime.now().isoformat(),
        "model": qwen_cloud.model
    })

@app.route('/api/chat/sync', methods=['POST'])
def chat_sync():
    """同步聊天接口"""
    try:
        data = request.json
        prompt = data.get('prompt', '')
        temperature = data.get('temperature', 0.7)

        if not prompt:
            return jsonify({"error": "prompt不能为空"}), 400

        start_time = time.time()
        response = qwen_cloud.generate_response(prompt, temperature=temperature)
        end_time = time.time()

        result = {
            "response": response,
            "processing_time": f"{end_time - start_time:.2f}秒",
            "timestamp": datetime.now().isoformat()
        }

        return jsonify(result)

    except Exception as e:
        logger.error(f"同步聊天错误: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chat/async', methods=['POST'])
def chat_async():
    """异步聊天接口（带回调）"""
    try:
        data = request.json
        prompt = data.get('prompt', '')
        callback_url = data.get('callback_url', '')
        task_id = data.get('task_id', f"task_{int(time.time())}")

        if not prompt:
            return jsonify({"error": "prompt不能为空"}), 400

        # 立即返回任务ID
        immediate_response = {
            "task_id": task_id,
            "status": "accepted",
            "message": "任务已接收，处理中...",
            "timestamp": datetime.now().isoformat()
        }

        # 异步处理
        def async_process():
            try:
                start_time = time.time()
                response = qwen_cloud.generate_response(prompt)
                end_time = time.time()

                result = {
                    "task_id": task_id,
                    "response": response,
                    "processing_time": f"{end_time - start_time:.2f}秒",
                    "status": "completed",
                    "timestamp": datetime.now().isoformat()
                }

                # 如果有回调URL，发送通知
                if callback_url:
                    qwen_cloud.notify_callbacks(task_id, result)

                logger.info(f"异步任务完成: {task_id}")

            except Exception as e:
                error_result = {
                    "task_id": task_id,
                    "error": str(e),
                    "status": "failed",
                    "timestamp": datetime.now().isoformat()
                }
                if callback_url:
                    qwen_cloud.notify_callbacks(task_id, error_result)
                logger.error(f"异步任务失败 {task_id}: {e}")

        # 启动异步处理线程
        thread = threading.Thread(target=async_process)
        thread.daemon = True
        thread.start()

        return jsonify(immediate_response)

    except Exception as e:
        logger.error(f"异步聊天错误: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/callback/register', methods=['POST'])
def register_callback():
    """注册回调处理器"""
    try:
        data = request.json
        callback_url = data.get('callback_url', '')

        if not callback_url:
            return jsonify({"error": "callback_url不能为空"}), 400

        qwen_cloud.register_callback(callback_url)

        return jsonify({
            "message": "回调处理器注册成功",
            "callback_url": callback_url,
            "total_handlers": len(qwen_cloud.callback_handlers)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """获取服务状态"""
    return jsonify({
        "service": "Qwen Cloud Deployment",
        "model": qwen_cloud.model,
        "registered_callbacks": len(qwen_cloud.callback_handlers),
        "callbacks": qwen_cloud.callback_handlers,
        "timestamp": datetime.now().isoformat()
    })

if __name__ == '__main__':
    # 启动云端服务
    print("=== Qwen 云端部署服务启动 ===")
    print("服务地址: http://0.0.0.0:8000")
    print("健康检查: http://localhost:8000/api/health")
    print("状态查询: http://localhost:8000/api/status")

    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)