# llm_extract.py
# 作用：把每个仓库的 README 交给 Deepseek，
# 让模型提取目标关系（relatedRepository, description），
# 每个仓库生成一段description，relatedRepository可以是一个或多个（如果有的话）
# 存到/home/byx/projects/OpenKG-GitHubRepository-KG/data/triples/llm_extracted_triples.csv
# 需要提前在 secrets 文件夹放置 deepseek_api_key.txt 和 deepseek_api_url.txt（如果没有则跳过）

import os
import json
import csv
from typing import Optional, Dict
from openai import OpenAI


class LLMExtractor:
    """使用 LLM（Deepseek）从 README 提取关系信息"""
    
    def __init__(self, base_path: str = "/home/byx/projects/OpenKG-GitHubRepository-KG"):
        self.base_path = base_path
        self.raw_data_path = os.path.join(base_path, "data/raw")
        self.triples_path = os.path.join(base_path, "data/triples")
        self.secrets_path = os.path.join(base_path, "secrets")
        
        # 创建输出目录
        os.makedirs(self.triples_path, exist_ok=True)
        
        # 尝试加载 API 配置
        self.api_key = self._load_secret("deepseek_api_key.txt")
        self.api_url = self._load_secret("deepseek_api_url.txt")
        self.available = self.api_key and self.api_url
        
        # 初始化 OpenAI 兼容客户端
        self.client = None
        if self.available:
            self.client = OpenAI(api_key=self.api_key, base_url=self.api_url)
        
        # 存储提取的三元组
        self.extracted_triples = []
    
    def _load_secret(self, filename: str) -> Optional[str]:
        """从 secrets 文件夹加载密钥或 URL"""
        file_path = os.path.join(self.secrets_path, filename)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception as e:
                print(f"Error reading {filename}: {e}")
        return None
    
    def _extract_with_llm(self, readme_content: str, repo_name: str) -> Dict:
        """使用 LLM 从 README 提取 description 和 relatedRepository"""
        if not self.available or not self.client:
            print(f"Skipping {repo_name}: API credentials not available")
            return {}
        
        prompt = f"""从以下 GitHub 仓库的 README 中提取以下信息，并以 JSON 格式返回：
1. description: 对该项目的简短描述（1-2 句）
2. relatedRepository: 如果 README 中提到的相关仓库，记录其url（列表，可以为空）

README 内容：
{readme_content[:100000]}  # 限制内容长度以避免 token 超限

请只返回 JSON，格式如下：
{{
    "description": "...",
    "relatedRepository": ["url_repo1", "url_repo2"]
}}
"""
        
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            content = response.choices[0].message.content
            if not content:
                print(f"Empty response content for {repo_name}")
                return {}
            
            # 清理可能的 ```json ``` 包裹
            content = content.strip()
            if content.startswith("```json"):
                content = content[7:]  # 删除 "```json"
            if content.startswith("```"):
                content = content[3:]  # 删除 "```"
            if content.endswith("```"):
                content = content[:-3]  # 删除结尾的 "```"
            content = content.strip()
            
            try:
                # 尝试解析 JSON
                extracted = json.loads(content)
                print(f"Content was: {content}...")
                return extracted
            except json.JSONDecodeError:
                # 如果不是有效的 JSON，返回空字典
                print(f"Failed to parse JSON response for {repo_name}")
                return {}
        
        except Exception as e:
            print(f"Error extracting from {repo_name}: {e}")
            return {}
    
    def process_repositories(self):
        """处理 raw 目录下所有仓库的 README"""
        for repo_dir in os.listdir(self.raw_data_path):
            repo_path = os.path.join(self.raw_data_path, repo_dir)
            
            if not os.path.isdir(repo_path):
                continue
            
            # 读取 README
            readme_file = os.path.join(repo_path, "README.md")
            if not os.path.exists(readme_file):
                print(f"Skipping {repo_dir}: no README.md found")
                continue
            
            try:
                with open(readme_file, 'r', encoding='utf-8') as f:
                    readme_content = f.read()
            except Exception as e:
                print(f"Error reading README for {repo_dir}: {e}")
                continue
            
            # 使用 LLM 提取信息
            extracted = self._extract_with_llm(readme_content, repo_dir)
            
            # 生成三元组
            if extracted:
                description = extracted.get("description", "")
                if description:
                    self.extracted_triples.append((repo_dir, "has_description", description))
                
                related_repos = extracted.get("relatedRepository", [])
                if isinstance(related_repos, list):
                    for related_repo in related_repos:
                        if related_repo:
                            self.extracted_triples.append((repo_dir, "has_related_repository", related_repo))
                
                print(f"Processed {repo_dir}: {len(self.extracted_triples)} triples so far")
            else:
                print(f"No information extracted from {repo_dir}")
    
    def save_triples(self):
        """保存提取的三元组到 CSV 文件"""
        triples_file = os.path.join(self.triples_path, "llm_extracted_triples.csv")
        with open(triples_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['subject', 'predicate', 'object'])
            for triple in self.extracted_triples:
                writer.writerow(triple)
        print(f"Saved {len(self.extracted_triples)} LLM-extracted triples to {triples_file}")
        print(f"Output file: {triples_file}")
    
    def run(self):
        """运行完整的 LLM 提取流程"""
        if not self.available:
            print("Warning: Deepseek API credentials not found in secrets/. Skipping LLM extraction.")
            return
        
        print("开始 LLM 提取...")
        print(f"Using API endpoint: {self.api_url}")
        self.process_repositories()
        self.save_triples()
        print("LLM 提取完成！")


if __name__ == "__main__":
    extractor = LLMExtractor()
    extractor.run()

