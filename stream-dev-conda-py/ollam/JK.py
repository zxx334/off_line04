import requests

response = requests.post(
    "http://localhost:11434/api/chat",
    json={
        "model": "qwen3:0.6b",
        "messages": [
            {"role": "user", "content": "用中文写一个简单的Python函数来计算斐波那契数列"}
        ],
        "stream": False
    }
)

print(response.json()["message"]["content"])