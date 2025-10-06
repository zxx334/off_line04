# fully_offline_finetune.py
import torch
import torch.nn as nn
from transformers import TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model
from datasets import Dataset
import json
import logging
import os
import numpy as np

# 完全禁用网络和警告
os.environ['HF_HUB_OFFLINE'] = '1'
os.environ['TRANSFORMERS_OFFLINE'] = '1'
os.environ['TRANSFORMERS_VERBOSITY'] = 'error'
os.environ['TOKENIZERS_PARALLELISM'] = 'false'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleTokenizer:
    """完全离线的简单分词器"""
    def __init__(self):
        # 创建基础词汇表
        self.vocab = {
            '[PAD]': 0, '[UNK]': 1, '[CLS]': 2, '[SEP]': 3, '[MASK]': 4,
            '<|im_start|>': 5, '<|im_end|>': 6, 'user': 7, 'assistant': 8,
            '这个': 9, '地址': 10, '在': 11, '哪个': 12, '省': 13, '市': 14,
            '区': 15, '县': 16, '请': 17, '标准化': 18, '北京': 19, '上海': 20,
            '广东': 21, '深圳': 22, '浙江': 23, '杭州': 24, '江苏': 25, '南京': 26,
            '四川': 27, '成都': 28, '陕西': 29, '西安': 30, '湖北': 31, '武汉': 32,
            '湖南': 33, '长沙': 34, '山东': 35, '济南': 36, '海淀': 37, '浦东': 38,
            '南山': 39, '西湖': 40, '鼓楼': 41, '武侯': 42, '雁塔': 43, '武昌': 44,
            '岳麓': 45, '历下': 46, '中关村': 47, '张江': 48, '科技园': 49, '大街': 50,
            '路': 51, '号': 52, '无法': 53, '确定': 54, '。': 55, '，': 56, '：': 57
        }
        self.inverse_vocab = {v: k for k, v in self.vocab.items()}
        self.pad_token_id = 0
        self.unk_token_id = 1
        self.cls_token_id = 2
        self.sep_token_id = 3

    def encode(self, text, max_length=128):
        """简单编码文本"""
        tokens = []
        # 简单的分词：按字符分割，但保留一些常见词汇
        words = text.replace('<|im_start|>', ' <|im_start|> ').replace('<|im_end|>', ' <|im_end|> ').split()

        for word in words:
            if word in self.vocab:
                tokens.append(self.vocab[word])
            else:
                # 对未知词按字符处理
                for char in word:
                    if char in self.vocab:
                        tokens.append(self.vocab[char])
                    else:
                        tokens.append(self.unk_token_id)

        # 截断或填充
        if len(tokens) > max_length:
            tokens = tokens[:max_length]
        else:
            tokens = tokens + [self.pad_token_id] * (max_length - len(tokens))

        return tokens

    def decode(self, token_ids):
        """解码token IDs"""
        tokens = []
        for token_id in token_ids:
            if token_id in self.inverse_vocab:
                tokens.append(self.inverse_vocab[token_id])
            else:
                tokens.append('[UNK]')
        return ' '.join(tokens)

class SimpleModel(nn.Module):
    """简单的Transformer模型"""
    def __init__(self, vocab_size=58, hidden_size=128, num_layers=4, num_heads=4):
        super().__init__()
        self.vocab_size = vocab_size
        self.hidden_size = hidden_size

        # 词嵌入
        self.embedding = nn.Embedding(vocab_size, hidden_size)

        # 简单的Transformer层
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_size,
            nhead=num_heads,
            dim_feedforward=hidden_size * 4,
            dropout=0.1,
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        # 输出层
        self.output = nn.Linear(hidden_size, vocab_size)

    def forward(self, input_ids, attention_mask=None, labels=None):
        # 嵌入
        x = self.embedding(input_ids)

        # Transformer
        x = self.transformer(x)

        # 输出
        logits = self.output(x)

        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss()
            loss = loss_fct(logits.view(-1, self.vocab_size), labels.view(-1))

        return {'loss': loss, 'logits': logits}

    def generate(self, input_ids, max_length=50, temperature=1.0):
        """简单的生成方法"""
        with torch.no_grad():
            for _ in range(max_length - input_ids.shape[1]):
                outputs = self.forward(input_ids)
                next_token_logits = outputs['logits'][:, -1, :] / temperature

                # 采样
                probabilities = torch.softmax(next_token_logits, dim=-1)
                next_token = torch.multinomial(probabilities, num_samples=1)

                input_ids = torch.cat([input_ids, next_token], dim=1)

        return input_ids

class FullyOfflineFinetuner:
    def __init__(self):
        self.tokenizer = None
        self.model = None
        self.lora_config = None

    def setup_model(self):
        """设置完全离线的模型"""
        logger.info("设置完全离线模型...")

        self.tokenizer = SimpleTokenizer()

        # 创建简单模型
        self.model = SimpleModel(
            vocab_size=len(self.tokenizer.vocab),
            hidden_size=128,
            num_layers=2,
            num_heads=4
        )

        logger.info(f"模型创建完成，词汇表大小: {len(self.tokenizer.vocab)}")

    def setup_lora(self):
        """配置LoRA"""
        logger.info("配置LoRA...")

        # 为简单模型配置LoRA
        self.lora_config = LoraConfig(
            r=4,
            lora_alpha=16,
            target_modules=["embedding", "output"],  # 针对简单模型的目标模块
            lora_dropout=0.1,
        )

        self.model = get_peft_model(self.model, self.lora_config)

        trainable_params = sum(p.numel() for p in self.model.parameters() if p.requires_grad)
        total_params = sum(p.numel() for p in self.model.parameters())
        logger.info(f"可训练参数: {trainable_params}/{total_params} ({trainable_params/total_params*100:.2f}%)")

    def load_training_data(self, data_path):
        """加载训练数据"""
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"加载了 {len(data)} 条训练数据")
        return data

    def preprocess_data(self, examples):
        """预处理数据"""
        input_ids = []
        labels = []

        for instruction, output in zip(examples["instruction"], examples["output"]):
            # 构建训练文本
            text = f"<|im_start|>user {instruction} <|im_end|> <|im_start|>assistant {output} <|im_end|>"

            # 编码
            tokens = self.tokenizer.encode(text)
            input_ids.append(tokens)
            labels.append(tokens.copy())  # 对于语言模型，标签就是输入

        return {
            "input_ids": input_ids,
            "labels": labels,
            "attention_mask": [[1] * len(ids) for ids in input_ids]
        }

    def train(self, data_path, output_dir):
        """执行训练"""
        logger.info("开始完全离线 LoRA 微调...")

        try:
            # 1. 设置模型
            self.setup_model()

            # 2. 配置LoRA
            self.setup_lora()

            # 3. 准备数据
            raw_data = self.load_training_data(data_path)
            dataset = Dataset.from_dict({
                "instruction": [item["instruction"] for item in raw_data],
                "output": [item["output"] for item in raw_data]
            })

            processed_dataset = dataset.map(
                self.preprocess_data,
                batched=True,
                remove_columns=dataset.column_names
            )

            logger.info(f"训练数据集大小: {len(processed_dataset)}")

            # 4. 训练参数
            training_args = TrainingArguments(
                output_dir=output_dir,
                num_train_epochs=5,
                per_device_train_batch_size=2,
                learning_rate=1e-3,
                logging_steps=1,
                save_steps=10,
                remove_unused_columns=False,
                report_to=None,
            )

            # 5. 训练器
            trainer = Trainer(
                model=self.model,
                args=training_args,
                train_dataset=processed_dataset,
            )

            # 6. 开始训练
            logger.info("启动训练过程...")
            train_result = trainer.train()

            # 7. 保存模型
            trainer.save_model()

            # 保存分词器信息
            tokenizer_info = {
                "vocab": self.tokenizer.vocab,
                "inverse_vocab": self.tokenizer.inverse_vocab
            }
            with open(os.path.join(output_dir, "tokenizer.json"), "w", encoding="utf-8") as f:
                json.dump(tokenizer_info, f, ensure_ascii=False, indent=2)

            logger.info(f"训练完成！模型保存到: {output_dir}")
            logger.info(f"最终训练损失: {train_result.metrics['train_loss']:.4f}")

            return True

        except Exception as e:
            logger.error(f"训练失败: {e}")
            import traceback
            traceback.print_exc()
            return False

if __name__ == "__main__":
    finetuner = FullyOfflineFinetuner()
    success = finetuner.train(
        data_path="training_data/qwen_address_training.json",
        output_dir="models/fully_offline_lora"
    )

    if success:
        print("🎉 完全离线 LoRA 微调成功完成！")
        print("模型保存在: models/fully_offline_lora")
    else:
        print("❌ 完全离线 LoRA 微调失败")