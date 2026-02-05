# rule_extract.py
# 作用：对抓下来的 README / meta 做规则抽取，生成候选三元组（含置信度）

import os, json, re
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/candidates")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# 你要固定的实体类型或实体名可以从 meta 中自动读取：
def read_all_repos():
    repos = []
    for owner_dir in RAW_DIR.iterdir():
        if not owner_dir.is_dir(): continue
        for repo_dir in owner_dir.iterdir():
            meta_path = repo_dir / "meta.json"
            if meta_path.exists():
                meta = json.load(open(meta_path, encoding="utf8"))
                readme_path = repo_dir / "README.md"
                readme = readme_path.read_text(encoding="utf8") if readme_path.exists() else ""
                repos.append((meta, readme))
    return repos

# 简单关键词/正则表（可扩展）
language_patterns = [
    r"written in ([A-Za-z#+\-]+)",
    r"implemented in ([A-Za-z#+\-]+)",
    r"language[:\s]+([A-Za-z#+\-]+)",
]
uses_patterns = [
    r"built with ([A-Za-z0-9\-\_\s/+.]+)",
    r"uses ([A-Za-z0-9\-\_\s/+.]+)",
    r"based on ([A-Za-z0-9\-\_\s/+.]+)"
]
developed_patterns = [
    r"developed by ([A-Za-z0-9\-\_\s]+)",
    r"maintained by ([A-Za-z0-9\-\_\s]+)"
]
release_patterns = [
    r"release v?([0-9]+\.[0-9]+(\.[0-9]+)?)",
    r"version[:\s]v?([0-9]+\.[0-9]+(\.[0-9]+)?)"
]

def apply_patterns(text, patterns):
    found = set()
    for p in patterns:
        for m in re.finditer(p, text, flags=re.I):
            g = m.group(1).strip()
            found.add(g)
    return list(found)

if __name__ == "__main__":
    repos = read_all_repos()
    rows = []
    for meta, readme in repos:
        repo_id = f"repo:{meta['owner']['login']}/{meta['name']}"
        # attributes from meta
        rows.append({"subject":repo_id,"predicate":"type","object":"GitHubRepository","score":1.0})
        rows.append({"subject":repo_id,"predicate":"name","object":meta.get("name"),"score":1.0})
        rows.append({"subject":repo_id,"predicate":"description","object":meta.get("description") or "", "score":1.0})
        rows.append({"subject":repo_id,"predicate":"stars","object":str(meta.get("stargazers_count",0)), "score":1.0})
        rows.append({"subject":repo_id,"predicate":"url","object":meta.get("html_url"), "score":1.0})
        # rule-based extractions from README
        text = (meta.get("description","") + "\n" + readme)[:20000]  # limit
        for lang in apply_patterns(text, language_patterns):
            rows.append({"subject":repo_id,"predicate":"writtenIn","object":lang,"score":0.8})
        for tech in apply_patterns(text, uses_patterns):
            rows.append({"subject":repo_id,"predicate":"usesTechnology","object":tech,"score":0.7})
        for dev in apply_patterns(text, developed_patterns):
            rows.append({"subject":repo_id,"predicate":"developedBy","object":dev,"score":0.7})
        for rel in apply_patterns(text, release_patterns):
            rows.append({"subject":repo_id,"predicate":"hasRelease","object":rel,"score":0.6})
    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "candidates.csv", index=False, encoding="utf8")
    print("candidates written:", len(df))
