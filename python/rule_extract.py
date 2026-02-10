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
        # releases 不再处理（按要求忽略 release 相关）
        self.contributors = set()
        
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
                # languages 在 fetch_repos.py 中已保存为列表
                'languages': metadata.get('languages', []),
                'url': metadata.get('url', '')
            }
            
            return entities
        except Exception as e:
            print(f"Error reading {metadata_path}: {e}")
            return {}
    
    def extract_from_readme(self, readme_path: str) -> Dict:
        """从README文件提取相关仓库信息（目前不提取额外实体）"""
        # 保留接口以便未来扩展
        return {}
    
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
                # 按仓库生成候选三元组（基于该仓库的实际信息），不做跨仓库笛卡尔积
                for lang in metadata_entities.get('languages', []):
                    if lang:
                        self.candidate_triples.append((repo_dir, 'uses_language', lang))
                lic = metadata_entities.get('license')
                if lic:
                    self.candidate_triples.append((repo_dir, 'has_license', lic))
                for tag in metadata_entities.get('tags', []):
                    if tag:
                        self.candidate_triples.append((repo_dir, 'has_tag', tag))
                # 星数三元组
                stars = metadata_entities.get('stars')
                if stars is not None:
                    self.candidate_triples.append((repo_dir, 'has_stars', stars))
                # 仓库链接三元组
                url = metadata_entities.get('url')
                if url:
                    self.candidate_triples.append((repo_dir, 'has_url', url))
                # 贡献者三元组
                for contrib in metadata_entities.get('contributors', []):
                    if contrib:
                        self.candidate_triples.append((repo_dir, 'has_contributor', contrib))
            # README 可选处理（目前不提取 releases）
            # readme_file = os.path.join(repo_path, 'README.md')
            # if os.path.exists(readme_file):
            #     _ = self.extract_from_readme(readme_file)
    
    def _collect_entities(self, entities: Dict):
        """收集提取到的实体"""
        if entities.get('license'):
            self.licenses.add(entities['license'])
        
        self.languages.update(entities.get('languages', []))
        self.tags.update(entities.get('tags', []))
        # 不再收集 releases
        # 收集贡献者为实体
        self.contributors.update(entities.get('contributors', []))
    
    def save_entities(self):
        """保存实体表到CSV文件"""
        entity_types = {
            'repositories': self.repositories,
            'licenses': self.licenses,
            'languages': self.languages,
            'tags': self.tags,
            'contributors': self.contributors,
            # releases 已忽略
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
    
    def generate_candidate_triples(self):
        """生成候选三元组"""
        # 直接保存已生成的候选三元组（不再做跨仓库笛卡尔积，也不生成 release 相关三元组）
        self._save_triples()
    
    def _save_triples(self):
        """保存候选三元组到CSV文件"""
        triples_file = os.path.join(self.triples_path, "candidate_triples.csv")
        with open(triples_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['subject', 'predicate', 'object'])
            for triple in self.candidate_triples:
                writer.writerow(triple)
        print(f"Saved {len(self.candidate_triples)} candidate triples to {triples_file}")
    
    def run(self):
        """运行完整的抽取流程"""
        print("开始规则抽取...")
        self.process_repositories()
        self.save_entities()
        self.generate_candidate_triples()
        print("规则抽取完成！")


if __name__ == "__main__":
    extractor = RuleExtractor()
    extractor.run()