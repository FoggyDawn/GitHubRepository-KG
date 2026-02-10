# rule_extract.py
# 作用：
# 1. 对data/raw目录内每个git的 README 和 metadata.json 做规则抽取，
# 抽取相关实体，实体包括：GitHub仓库、开源许可证、编程语言、标签、版本release。
# 2.给每种实体构建实例表。每种实体1个实例表，
# 置于 data/entities/ 目录下，命名为 {entity_type}_entities.csv
# 表内每行仅包含实体实例的名称（字符串），不包含其他属性。
# 3.生成候选三元组
# 参考schema/prperties.json内的属性定义，生成候选三元组，
# 置于 data/triples/ 目录下，命名为 candidate_triples.csv

import os
import json
import csv
from pathlib import Path
from typing import Set, Dict, List, Tuple, Optional

class RuleExtractor:
    def __init__(self, base_path: str = "/home/byx/projects/OpenKG-GitHubRepository-KG"):
        self.base_path = base_path
        self.raw_data_path = os.path.join(base_path, "data/raw")
        self.entities_path = os.path.join(base_path, "data/entities")
        self.triples_path = os.path.join(base_path, "data/triples")
        
        # 创建输出目录
        os.makedirs(self.entities_path, exist_ok=True)
        os.makedirs(self.triples_path, exist_ok=True)
        
        # 实体集合
        self.repositories = set()
        self.licenses = set()
        self.languages = set()
        self.tags = set()
        self.releases = set()
        
        # 候选三元组列表
        self.candidate_triples = []
    
    def extract_from_metadata(self, metadata_path: str, repo_name: str) -> Dict:
        """从metadata.json文件提取实体信息"""
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            entities = {
                'repository': repo_name,
                'stars': metadata.get('stars', 0),
                'contributors': metadata.get('contributors', []),
                'license': metadata.get('license', ''),
                'tags': metadata.get('topics', []),
                'releases': metadata.get('releases', [])
            }
            
            return entities
        except Exception as e:
            print(f"Error reading {metadata_path}: {e}")
            return {}
    
    def extract_from_readme(self, readme_path: str) -> Dict:
        """从README文件提取相关仓库和使用语言等信息"""
        entities = {'tags': [], 'releases': []}
        try:
            with open(readme_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 简单的版本提取逻辑（可根据实际需求优化）
            import re
            version_pattern = r'v?\d+\.\d+\.\d+'
            releases = re.findall(version_pattern, content)
            entities['releases'] = list(set(releases))
            
        except Exception as e:
            print(f"Error reading {readme_path}: {e}")
        
        return entities
    
    def process_repositories(self):
        """处理raw目录下的所有仓库"""
        for repo_dir in os.listdir(self.raw_data_path):
            repo_path = os.path.join(self.raw_data_path, repo_dir)
            
            if not os.path.isdir(repo_path):
                continue
            
            # 添加仓库名称
            self.repositories.add(repo_dir)
            
            # 处理metadata.json
            metadata_file = os.path.join(repo_path, "metadata.json")
            if os.path.exists(metadata_file):
                metadata_entities = self.extract_from_metadata(metadata_file, repo_dir)
                self._collect_entities(metadata_entities)
            
            # 处理README
            readme_file = os.path.join(repo_path, "README.md")
            if os.path.exists(readme_file):
                readme_entities = self.extract_from_readme(readme_file)
                self.tags.update(readme_entities.get('tags', []))
                self.releases.update(readme_entities.get('releases', []))
    
    def _collect_entities(self, entities: Dict):
        """收集提取到的实体"""
        if entities.get('license'):
            self.licenses.add(entities['license'])
        
        self.languages.update(entities.get('languages', []))
        self.tags.update(entities.get('tags', []))
        self.releases.update(entities.get('releases', []))
    
    def save_entities(self):
        """保存实体表到CSV文件"""
        entity_types = {
            'repositories': self.repositories,
            'licenses': self.licenses,
            'languages': self.languages,
            'tags': self.tags,
            'releases': self.releases
        }
        
        for entity_type, entities in entity_types.items():
            file_path = os.path.join(self.entities_path, f"{entity_type}_entities.csv")
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['name'])
                for entity in sorted(entities):
                    if entity:  # 跳过空值
                        writer.writerow([entity])
            print(f"Saved {len(entities)} {entity_type} to {file_path}")
    
    def generate_candidate_triples(self, schema_path: Optional[str] = None):
        """生成候选三元组"""
        # 加载属性定义（如果提供了schema_path）
        properties = self._load_properties(schema_path)
        
        # 生成候选三元组
        for repo in self.repositories:
            for lang in self.languages:
                self.candidate_triples.append((repo, 'uses_language', lang))
            
            for license in self.licenses:
                self.candidate_triples.append((repo, 'has_license', license))
            
            for tag in self.tags:
                self.candidate_triples.append((repo, 'has_tag', tag))
            
            for release in self.releases:
                self.candidate_triples.append((repo, 'has_release', release))
        
        self._save_triples()
    
    def _load_properties(self, schema_path: Optional[str] = None) -> Dict:
        """加载属性定义"""
        if schema_path and os.path.exists(schema_path):
            try:
                with open(schema_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading schema: {e}")
        return {}
    
    def _save_triples(self):
        """保存候选三元组到CSV文件"""
        triples_file = os.path.join(self.triples_path, "candidate_triples.csv")
        with open(triples_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['subject', 'predicate', 'object'])
            for triple in self.candidate_triples:
                writer.writerow(triple)
        print(f"Saved {len(self.candidate_triples)} candidate triples to {triples_file}")
    
    def run(self, schema_path: Optional[str] = None):
        """运行完整的抽取流程"""
        print("开始规则抽取...")
        self.process_repositories()
        self.save_entities()
        self.generate_candidate_triples(schema_path)
        print("规则抽取完成！")


if __name__ == "__main__":
    extractor = RuleExtractor()
    # 如果有schema文件，传入schema_path参数
    schema_path = "/home/byx/projects/OpenKG-GitHubRepository-KG/schema/properties.json"
    extractor.run(schema_path)