from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
import torch

class FinetunedModelTester:
    def __init__(self, base_model_path, lora_model_path):
        self.base_model_path = base_model_path
        self.lora_model_path = lora_model_path
        self.tokenizer = None
        self.model = None

    def load_model(self):
        """加载微调后的模型"""
        print("正在加载微调模型...")

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.base_model_path,
            trust_remote_code=True
        )

        base_model = AutoModelForCausalLM.from_pretrained(
            self.base_model_path,
            trust_remote_code=True,
            torch_dtype=torch.float16,
            device_map="auto"
        )

        self.model = PeftModel.from_pretrained(
            base_model,
            self.lora_model_path
        )

        print("模型加载完成")

    def generate_response(self, prompt, max_length=512):
        """生成回复"""
        # 构建对话格式
        text = f"<|im_start|>user\n{prompt}<|im_end|>\n<|im_start|>assistant\n"

        inputs = self.tokenizer(text, return_tensors="pt")
        inputs = {k: v.to(self.model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_length=max_length,
                temperature=0.7,
                do_sample=True,
                pad_token_id=self.tokenizer.eos_token_id
            )

        response = self.tokenizer.decode(outputs[0], skip_special_tokens=False)
        # 提取assistant的回复
        if "<|im_start|>assistant\n" in response:
            response = response.split("<|im_start|>assistant\n")[-1]
            if "<|im_end|>" in response:
                response = response.split("<|im_end|>")[0]

        return response.strip()

    def test_address_queries(self):
        """测试地址相关查询"""
        test_cases = [
            "这个地址在哪个省？北京市海淀区中关村大街1号",
            "请标准化这个地址：上海浦东新区张江高科技园区",
            "广东省深圳市在哪个省？",
            "补全这个地址的区县信息：江苏省南京市",
            "这个地址在哪个区？杭州市西湖区文三路100号"
        ]

        print("=== 测试微调后的模型 ===")

        for i, query in enumerate(test_cases, 1):
            print(f"\n测试 {i}: {query}")
            response = self.generate_response(query)
            print(f"模型回复: {response}")

if __name__ == "__main__":
    tester = FinetunedModelTester(
        base_model_path="Qwen/Qwen2.5-0.5B",
        lora_model_path="./qwen_address_lora_finetuned"
    )

    tester.load_model()
    tester.test_address_queries()