# deepseek_extract.py
# 作用：把 README 里长句子交给 Deepseek，让模型提取目标关系（developedBy, writtenIn, usesTechnology, applicationDomain, relatedRepository, hasRelease）

import os, requests, json, time
from pathlib import Path
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam

# 从仓库内的 secrets 文件夹读取 deepseek 配置（文件名：deepseek_api_key.txt, deepseek_api_url.txt）
script_dir = os.path.dirname(os.path.abspath(__file__))
secrets_dir = os.path.normpath(os.path.join(script_dir, '..', 'secrets'))
key_file = os.path.join(secrets_dir, 'deepseek_api_key.txt')
url_file = os.path.join(secrets_dir, 'deepseek_api_url.txt')
if not os.path.isfile(key_file) or not os.path.isfile(url_file):
    print(f"Deepseek 未配置（缺少 {key_file} 或 {url_file}），跳过 LLM 提取")
    exit(0)
with open(key_file, 'r', encoding='utf-8') as f:
    DEEPSEEK_API_KEY = f.read().strip()
with open(url_file, 'r', encoding='utf-8') as f:
    DEEPSEEK_API_URL = f.read().strip()
if not (DEEPSEEK_API_KEY and DEEPSEEK_API_URL):
    print("Deepseek 配置为空，跳过 LLM 提取")
    exit(0)

HEADERS = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}

# Prompt 模板（few-shot）
PROMPT_TEMPLATE = """
你是一个关系抽取器。目标关系集合：
- developedBy(软件 -> 人/组织)
- writtenIn(软件 -> 编程语言)
- usesTechnology(软件 -> 技术/框架)
- applicationDomain(软件 -> 应用领域)
- relatedRepository(软件 -> 软件仓库)
- hasRelease(软件 -> 版本号)

输入：一段 README 文本 和 仓库 名称。
请以 JSON 输出，格式：
[
  {"predicate":"developedBy", "object":"OpenAI", "span":"...text span...", "confidence":0.9},
  ...
]

示例：
输入: repo=vllm, text="vLLM is an inference engine developed by OpenAI. It is written in Python and uses CUDA and Transformers."
输出: [{"predicate":"developedBy","object":"OpenAI","span":"developed by OpenAI","confidence":0.98},
         {"predicate":"writtenIn","object":"Python","span":"written in Python","confidence":0.96},
         {"predicate":"usesTechnology","object":"CUDA","span":"uses CUDA","confidence":0.9},
         {"predicate":"usesTechnology","object":"Transformers","span":"uses Transformers","confidence":0.9}
        ]

现在输入:
repo={repo}
text: \"\"\"{text}\"\"\"
请仅返回 JSON 数组。
"""

def call_deepseek(repo, text):
    # 使用 OpenAI 官方客户端调用 deepseek 模型（要求已安装 openai 包）
    prompt_text = PROMPT_TEMPLATE.format(repo=repo, text=text[:4000])
    messages: list[ChatCompletionMessageParam] = [
        {"role": "system", "content": "你是一个关系抽取器，接收用户输入并仅以 JSON 数组返回结果。"},
        {"role": "user", "content": prompt_text}
    ]

    client = OpenAI(api_key=DEEPSEEK_API_KEY)
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            max_tokens=800,
            temperature=0.0
        )
    except Exception as e:
        raise

    # 兼容不同实现：优先从 chat response 中取 message content
    content = None
    try:
        choices = resp.get('choices') if isinstance(resp, dict) else None
        if not choices and hasattr(resp, 'choices'):
            choices = getattr(resp, 'choices')
        choices = choices or []
        if choices and isinstance(choices, list):
            first = choices[0]
            # OpenAI Chat format
            if isinstance(first, dict):
                content = first.get('message', {}).get('content') if isinstance(first.get('message'), dict) else None
                if content is None:
                    content = first.get('text')
            else:
                # 可能是对象，尝试获取属性
                content = getattr(first, 'message', None)
                if content and isinstance(content, dict):
                    content = content.get('content')
                if content is None:
                    content = getattr(first, 'text', None)
    except Exception:
        content = None

    if content is None:
        # 有的服务直接返回文本在 top-level 字段
        if isinstance(resp, dict):
            content = resp.get('output') or resp.get('text') or json.dumps(resp)
        else:
            # 尝试 str 转换
            content = str(resp)

    # 尝试将返回内容解析为 JSON 数组
    try:
        return json.loads(content)
    except Exception:
        # 容错：尝试从 content 中抽取第一个 JSON 数组片段
        try:
            start = content.index('[')
            end = content.rindex(']')
            candidate = content[start:end+1]
            return json.loads(candidate)
        except Exception as e:
            raise ValueError(f"无法解析 deepseek 返回的内容为 JSON: {e}\nraw: {content}")

if __name__ == "__main__":
    # 读取之前的 README（基于脚本目录构造路径）
    raw = Path(os.path.join(script_dir, '..', 'data', 'raw'))
    out_rows = []
    for owner in raw.iterdir():
        for repo_dir in owner.iterdir():
            readme_path = repo_dir / "README.md"
            meta_path = repo_dir / "meta.json"
            if not meta_path.exists(): continue
            repo_id = f"repo:{owner.name}/{repo_dir.name}"
            text = readme_path.read_text(encoding="utf8") if readme_path.exists() else ""
            if not text.strip(): continue
            try:
                res = call_deepseek(repo_id, text)
            except Exception as e:
                print("Deepseek 调用失败:", e)
                continue
            # 解析返回（这里按示例结构）
            for item in res:
                predicate = item.get("predicate")
                obj = item.get("object")
                conf = item.get("confidence", 0.8)
                out_rows.append({"subject":repo_id, "predicate":predicate, "object":obj, "score":conf})
            time.sleep(1)  # 慎用
    # 保存
    import pandas as pd
    out_path = Path(os.path.join(script_dir, '..', 'data', 'candidates_from_deepseek.csv'))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(out_path, index=False, encoding="utf8")
    print("deepseek extraction saved")
