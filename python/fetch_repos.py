# fetch_repos.py
# 作用：用 GitHub Search API 抓取高 star 仓库（前200），并保存 README 原文
# 和仓库元数据（包括名称、星标、开发者名单、许可证类型、标签、各个发布版本）
# 到 ./data/raw/，每个仓库一个文件夹，文件夹名称为 {owner}_{repo_name}

import os
import requests
import json
from pathlib import Path
import time
from requests.exceptions import ConnectionError, Timeout

# 获取 GitHub token（基于脚本目录计算相对路径，避免求绝对路径时出错）
script_dir = os.path.dirname(os.path.abspath(__file__))
token_path = os.path.normpath(os.path.join(script_dir, '..', 'secrets', 'github_token.txt'))
if not os.path.isfile(token_path):
    raise ValueError(f"请确保 {token_path} 文件存在并包含 GitHub token")
with open(token_path, 'r') as f:
    GITHUB_TOKEN = f.read().strip()

if not GITHUB_TOKEN:
    raise ValueError("GitHub token 不能为空")

headers = {'Authorization': f'token {GITHUB_TOKEN}'}
base_url = 'https://api.github.com'


def make_request(url, headers=None, max_retries=3, timeout=10)->requests.Response: # pyright: ignore[reportReturnType]
    """带重试机制的请求函数"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response is not None:
                return response
        except (ConnectionError, Timeout) as e:
            if attempt < max_retries - 1:
                print(f"请求失败，重试 {attempt+1}/{max_retries}: {e}")
                time.sleep(2 ** attempt)  # 指数退避
            else:
                raise

def get_top_repos(limit=200):
    """获取前 limit 个高星标仓库"""
    repos = []
    page = 1
    per_page = 100
    while len(repos) < limit:
        url = f'{base_url}/search/repositories?q=stars:>1&sort=stars&order=desc&page={page}&per_page={per_page}'
        try:
            response = make_request(url, headers)
        except Exception as e:
            print(f"获取仓库列表失败: {e}")
            break
        if response.status_code != 200: # pyright: ignore[reportOptionalMemberAccess]
            print(f"获取仓库列表失败: {response.status_code} - {response.text}") # pyright: ignore[reportOptionalMemberAccess]
            break
        data = response.json()
        repos.extend(data['items'])
        if len(data['items']) < per_page:
            break
        page += 1
    return repos[:limit]

def get_repo_data(owner, repo):
    """获取仓库的元数据和 README"""
    # 获取仓库基本信息
    repo_url = f'{base_url}/repos/{owner}/{repo}'
    try:
        repo_response = make_request(repo_url, headers)
    except Exception as e:
        print(f"获取仓库 {owner}/{repo} 信息失败: {e}")
        return None, ""
    if repo_response.status_code != 200: # type: ignore
        print(f"获取仓库 {owner}/{repo} 信息失败: {repo_response.status_code}")
        return None, ""
    
    repo_data = repo_response.json()
    
    # 获取 README
    readme_content = ""
    readme_url = f'{base_url}/repos/{owner}/{repo}/readme'
    try:
        readme_response = make_request(readme_url, headers)
    except Exception as e:
        print(f"获取 README 失败: {e}")
        readme_response = None
    if readme_response and readme_response.status_code == 200:
        readme_data = readme_response.json()
        try:
            download_response = make_request(readme_data['download_url'], headers)
        except Exception as e:
            print(f"下载 README 失败: {e}")
            download_response = None
        if download_response and download_response.status_code == 200:
            readme_content = download_response.text
    
    # 获取贡献者
    contributors = []
    contributors_url = f'{base_url}/repos/{owner}/{repo}/contributors'
    page = 1
    while True:
        try:
            response = make_request(f'{contributors_url}?page={page}&per_page=100', headers)
        except Exception as e:
            print(f"获取贡献者失败: {e}")
            break
        if response.status_code != 200: # pyright: ignore[reportOptionalMemberAccess]
            break
        data = response.json()
        if not data:
            break
        contributors.extend([c['login'] for c in data])
        page += 1
    
    # 获取许可证
    license_type = repo_data.get('license', {}).get('name') if repo_data.get('license') else None
    
    # 获取主语言和所有语言分布
    primary_language = repo_data.get('language')
    languages = []
    languages_url = f'{base_url}/repos/{owner}/{repo}/languages'
    try:
        lang_resp = make_request(languages_url, headers)
        if lang_resp and lang_resp.status_code == 200:
            lang_json = lang_resp.json()
            languages = list(lang_json.keys())
    except Exception as e:
        print(f"获取语言分布失败: {e}")
        languages = []
    
    # 获取标签（topics）
    topics = repo_data.get('topics', [])
    
    # 获取发布版本
    releases = []
    releases_url = f'{base_url}/repos/{owner}/{repo}/releases'
    page = 1
    while True:
        try:
            response = make_request(f'{releases_url}?page={page}&per_page=100', headers)
        except Exception as e:
            print(f"获取发布版本失败: {e}")
            break
        if response.status_code != 200: # pyright: ignore[reportOptionalMemberAccess]
            break
        data = response.json()
        if not data:
            break
        releases.extend([r['tag_name'] for r in data])
        page += 1
    
    metadata = {
        'name': repo,
        'stars': repo_data['stargazers_count'],
        'contributors': contributors,
        'license': license_type,
        'url': repo_data.get('html_url', f'https://github.com/{owner}/{repo}'),
        'primary_language': primary_language,
        'languages': languages,
        'topics': topics,
        'releases': releases
    }
    
    return metadata, readme_content

def main():
    # 使用脚本目录计算 data/raw 的路径，避免在某些环境下 Path.resolve 出错
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = Path(os.path.join(script_dir, '..', 'data', 'raw'))
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    repos = get_top_repos()
    print(f"获取到 {len(repos)} 个仓库")
    
    for i, repo in enumerate(repos):
        owner = repo['owner']['login']
        repo_name = repo['name']
        folder_name = f'{owner}_{repo_name}'
        folder_path = raw_dir / folder_name
        folder_path.mkdir(exist_ok=True)
        
        print(f"处理 {i+1}/{len(repos)}: {owner}/{repo_name}")
        
        metadata, readme = get_repo_data(owner, repo_name)
        if metadata is None:
            continue
        
        # 保存元数据
        with open(folder_path / 'metadata.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        
        # 保存 README
        with open(folder_path / 'README.md', 'w', encoding='utf-8') as f:
            f.write(readme)

if __name__ == '__main__':
    main()
