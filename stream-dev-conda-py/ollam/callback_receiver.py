from flask import Flask, request, jsonify
import json
from datetime import datetime

app = Flask(__name__)

@app.route('/callback/receive', methods=['POST'])
def receive_callback():
    """接收回调通知的示例端点"""
    try:
        data = request.json
        print(f"\n=== 收到回调通知 ===")
        print(f"任务ID: {data.get('task_id')}")
        print(f"状态: {data.get('status')}")
        print(f"时间: {data.get('timestamp')}")

        if data.get('status') == 'completed':
            print(f"响应内容: {data.get('result', {}).get('response', '')}")
        else:
            print(f"错误: {data.get('error', '未知错误')}")

        print("=== 回调处理完成 ===\n")

        return jsonify({"status": "callback_received"})

    except Exception as e:
        print(f"回调处理错误: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "callback_receiver_ready"})

if __name__ == '__main__':
    print("=== 回调接收器启动 ===")
    print("服务地址: http://0.0.0.0:9000")
    print("回调端点: http://localhost:9000/callback/receive")

    app.run(host='0.0.0.0', port=9000, debug=True)