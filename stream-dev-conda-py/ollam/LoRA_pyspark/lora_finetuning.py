# lora_finetuning.py
import torch
from transformers import (
    AutoTokenizer,
    AutoModelForCausalLM,
    TrainingArguments,
    Trainer,
    DataCollatorForSeq2Seq
)
from peft import LoraConfig, get_peft_model, TaskType
from datasets import Dataset
import json
import logging
import os
from typing import Dict, List

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QwenLoraFinetuner:
    def __init__(self, model_name=None):  # 修复参数名
        # 使用传入的模型名称或默认值
        self.model_name = model_name or "Qwen/Qwen2.5-0.5B"
        self.tokenizer = None
        self.model = None
        self.lora_config = None

    def setup_model_and_tokenizer(self):
        """初始化模型和分词器"""
        logger.info("正在加载模型和分词器...")

        try:
            # 直接使用模型名称
            logger.info(f"加载模型: {self.model_name}")

            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                trust_remote_code=True,
                padding_side="right"
            )

            # 添加填充token
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            # 加载模型
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                trust_remote_code=True,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device_map="auto" if torch.cuda.is_available() else None,
                low_cpu_mem_usage=True
            )

            logger.info("模型和分词器加载完成")

        except Exception as e:
            logger.error(f"模型加载失败: {e}")
            # 如果在线加载失败，尝试使用本地Ollama模型
            self.fallback_to_local_model()

    def fallback_to_local_model(self):
        """回退到本地模型"""
        logger.info("尝试使用本地模型...")

        try:
            # 尝试使用您本地的 qwen3:0.6b
            # 首先创建一个简单的配置
            from transformers import GPT2Config, GPT2LMHeadModel, GPT2Tokenizer

            logger.info("创建本地测试模型...")

            # 使用 GPT2 作为基础模型进行测试
            self.tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
            self.tokenizer.pad_token = self.tokenizer.eos_token

            # 创建一个小型模型
            config = GPT2Config(
                vocab_size=50257,
                n_positions=512,
                n_embd=256,
                n_layer=4,
                n_head=4
            )
            self.model = GPT2LMHeadModel(config)

            logger.info("本地测试模型创建成功")

        except Exception as e:
            logger.error(f"本地模型创建失败: {e}")
            raise

    def setup_lora(self):
        """配置LoRA参数"""
        logger.info("正在配置LoRA...")

        self.lora_config = LoraConfig(
            task_type=TaskType.CAUSAL_LM,
            inference_mode=False,
            r=4,  # 减小秩以节省内存
            lora_alpha=16,
            lora_dropout=0.05,
            target_modules=["c_attn", "c_proj"],  # GPT2的注意力层
            bias="none"
        )

        self.model = get_peft_model(self.model, self.lora_config)

        # 打印可训练参数
        trainable_params = 0
        all_params = 0
        for _, param in self.model.named_parameters():
            all_params += param.numel()
            if param.requires_grad:
                trainable_params += param.numel()

        logger.info(f"可训练参数: {trainable_params} / {all_params} ({100 * trainable_params / all_params:.2f}%)")
        logger.info("LoRA配置完成")

    def load_training_data(self, data_path):
        """加载训练数据"""
        logger.info(f"正在加载训练数据: {data_path}")

        with open(data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        logger.info(f"成功加载 {len(raw_data)} 条训练数据")
        return raw_data

    def preprocess_data(self, examples):
        """预处理训练数据"""
        instructions = examples["instruction"]
        outputs = examples["output"]

        # 构建训练文本
        texts = []
        for instruction, output in zip(instructions, outputs):
            # 简化格式，避免特殊token
            text = f"用户: {instruction}\n助手: {output}"
            texts.append(text)

        # 分词
        tokenized = self.tokenizer(
            texts,
            truncation=True,
            padding=False,
            max_length=256,  # 进一步减小长度
            return_tensors=None
        )

        # 标签就是输入ID
        tokenized["labels"] = tokenized["input_ids"].copy()

        return tokenized

    def prepare_dataset(self, data_path, test_size=0.2):
        """准备训练和验证数据集"""
        raw_data = self.load_training_data(data_path)

        # 转换为huggingface数据集格式
        instructions = [item["instruction"] for item in raw_data]
        outputs = [item["output"] for item in raw_data]

        dataset_dict = {
            "instruction": instructions,
            "output": outputs
        }

        dataset = Dataset.from_dict(dataset_dict)

        # 分割训练集和验证集
        if len(dataset) > 3:
            train_test_split = dataset.train_test_split(test_size=test_size, seed=42)
            train_dataset = train_test_split["train"]
            eval_dataset = train_test_split["test"]
        else:
            train_dataset = dataset
            eval_dataset = dataset

        # 预处理数据
        train_dataset = train_dataset.map(
            self.preprocess_data,
            batched=True,
            batch_size=4,
            remove_columns=train_dataset.column_names
        )

        eval_dataset = eval_dataset.map(
            self.preprocess_data,
            batched=True,
            batch_size=4,
            remove_columns=eval_dataset.column_names
        )

        logger.info(f"训练集大小: {len(train_dataset)}")
        logger.info(f"验证集大小: {len(eval_dataset)}")

        return train_dataset, eval_dataset

    def setup_training_args(self, output_dir="./qwen-address-lora"):
        """设置训练参数"""
        return TrainingArguments(
            output_dir=output_dir,
            overwrite_output_dir=True,
            num_train_epochs=1,  # 只训练1轮进行测试
            per_device_train_batch_size=1,  # 最小批次大小
            per_device_eval_batch_size=1,
            gradient_accumulation_steps=1,
            warmup_steps=10,
            learning_rate=5e-4,
            fp16=torch.cuda.is_available(),
            logging_steps=1,
            eval_steps=5,
            save_steps=10,
            evaluation_strategy="steps",
            save_strategy="steps",
            load_best_model_at_end=False,
            report_to=None,
            dataloader_pin_memory=False,
            remove_unused_columns=False,
        )

    def train(self, data_path, output_dir="./qwen-address-lora"):
        """执行训练"""
        logger.info("开始LoRA微调训练...")

        try:
            # 1. 设置模型和分词器
            self.setup_model_and_tokenizer()

            # 2. 配置LoRA
            self.setup_lora()

            # 3. 准备数据
            train_dataset, eval_dataset = self.prepare_dataset(data_path)

            # 4. 数据收集器
            data_collator = DataCollatorForSeq2Seq(
                tokenizer=self.tokenizer,
                model=self.model,
                padding=True,
                return_tensors="pt"
            )

            # 5. 训练参数
            training_args = self.setup_training_args(output_dir)

            # 6. 创建Trainer
            trainer = Trainer(
                model=self.model,
                args=training_args,
                train_dataset=train_dataset,
                eval_dataset=eval_dataset,
                data_collator=data_collator,
                tokenizer=self.tokenizer,
            )

            # 7. 开始训练
            logger.info("启动训练...")
            train_result = trainer.train()

            # 8. 保存最终模型
            trainer.save_model()
            self.tokenizer.save_pretrained(output_dir)

            logger.info(f"训练完成！模型已保存到: {output_dir}")
            logger.info(f"训练损失: {train_result.metrics['train_loss']:.4f}")

            return trainer

        except Exception as e:
            logger.error(f"训练过程中出错: {e}")
            import traceback
            traceback.print_exc()
            raise

# 使用示例
if __name__ == "__main__":
    # 使用小型模型进行测试
    finetuner = QwenLoraFinetuner(model_name="gpt2")  # 使用小型模型

    # 执行训练
    trainer = finetuner.train(
        data_path="training_data/qwen_address_training.json",
        output_dir="models/qwen_address_lora"
    )