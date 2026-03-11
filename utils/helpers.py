"""
辅助工具函数
"""
import re
import random
import asyncio
import json
from datetime import datetime
from typing import Optional, Tuple
from loguru import logger


def parse_salary(salary_str: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    解析薪资字符串，返回 (下限, 上限, 单位)
    例如: "15-25K·13薪" -> (15.0, 25.0, "千元/月")
         "2-3万/月" -> (20.0, 30.0, "千元/月")
         "200-300元/天" -> (None, None, "元/天")
    """
    if not salary_str or salary_str in ["面议", "薪资面议", "待遇面议", ""]:
        return None, None, "面议"

    salary_str = salary_str.strip()
    unit = "千元/月"

    # 匹配 "X-YK" 或 "X-Y千" 格式（千元）
    match = re.search(r'(\d+\.?\d*)\s*[-~]\s*(\d+\.?\d*)\s*[Kk千]', salary_str)
    if match:
        low = float(match.group(1))
        high = float(match.group(2))
        return low, high, unit

    # 匹配 "X-Y万" 格式
    match = re.search(r'(\d+\.?\d*)\s*[-~]\s*(\d+\.?\d*)\s*万', salary_str)
    if match:
        low = float(match.group(1)) * 10
        high = float(match.group(2)) * 10
        return low, high, unit

    # 匹配单值 "XK" 格式
    match = re.search(r'(\d+\.?\d*)\s*[Kk千]', salary_str)
    if match:
        val = float(match.group(1))
        return val, val, unit

    # 匹配单值 "X万" 格式
    match = re.search(r'(\d+\.?\d*)\s*万', salary_str)
    if match:
        val = float(match.group(1)) * 10
        return val, val, unit

    # 匹配元/天格式
    match = re.search(r'(\d+\.?\d*)\s*[-~]\s*(\d+\.?\d*)\s*元', salary_str)
    if match:
        return float(match.group(1)), float(match.group(2)), "元/天"

    return None, None, salary_str


def clean_text(text: str) -> str:
    """清理文本，去除多余空白字符"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    text = text.replace('\u200b', '').replace('\xa0', ' ')
    return text


def parse_experience(exp_str: str) -> str:
    """标准化经验要求"""
    if not exp_str:
        return "不限"
    exp_str = clean_text(exp_str)
    mappings = {
        "不限": "不限",
        "应届": "应届生",
        "在校": "在校生",
        "1年以下": "1年以下",
        "1-3年": "1-3年",
        "3-5年": "3-5年",
        "5-10年": "5-10年",
        "10年以上": "10年以上",
    }
    for key, val in mappings.items():
        if key in exp_str:
            return val
    return exp_str


def parse_education(edu_str: str) -> str:
    """标准化学历要求"""
    if not edu_str:
        return "不限"
    edu_str = clean_text(edu_str)
    for level in ["博士", "硕士", "本科", "大专", "中专", "高中", "不限"]:
        if level in edu_str:
            return level
    return edu_str


def parse_company_size(size_str: str) -> str:
    """标准化公司规模"""
    if not size_str:
        return "未知"
    size_str = clean_text(size_str)
    for pattern in ["20人以下", "20-99人", "100-499人", "500-999人",
                    "1000-9999人", "10000人以上", "少于15人", "15-50人",
                    "50-150人", "150-500人", "500-2000人", "2000人以上"]:
        if pattern in size_str:
            return pattern
    return size_str


async def random_delay(min_sec: float = 1.0, max_sec: float = 3.0):
    """随机延迟，模拟人类行为"""
    delay = random.uniform(min_sec, max_sec)
    await asyncio.sleep(delay)


async def human_scroll(page, times: int = 3):
    """模拟人类滚动页面行为"""
    for _ in range(times):
        scroll_y = random.randint(300, 600)
        await page.evaluate(f"window.scrollBy(0, {scroll_y})")
        await asyncio.sleep(random.uniform(0.3, 0.8))


async def human_mouse_move(page):
    """模拟人类鼠标移动"""
    try:
        x = random.randint(100, 800)
        y = random.randint(100, 600)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
    except Exception:
        pass


def format_datetime(dt=None) -> str:
    """格式化日期时间"""
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def safe_json_loads(text: str) -> Optional[dict]:
    """安全解析JSON"""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def extract_skills_from_description(description: str) -> str:
    """从职位描述中提取技能关键词"""
    skills_keywords = [
        "Python", "SQL", "Excel", "Tableau", "Power BI", "R语言",
        "SPSS", "SAS", "Hadoop", "Spark", "Hive", "Flink",
        "机器学习", "深度学习", "数据仓库", "ETL", "数据可视化",
        "A/B测试", "用户画像", "MySQL", "PostgreSQL", "MongoDB",
        "Presto", "ClickHouse", "数据建模", "统计分析", "Java",
        "Scala", "Kafka", "数据治理", "BI", "报表", "看板",
    ]
    found_skills = []
    if description:
        for skill in skills_keywords:
            if skill.lower() in description.lower():
                found_skills.append(skill)
    return "、".join(found_skills)


def normalize_city(city_str: str) -> str:
    """标准化城市名称"""
    if not city_str:
        return "未知"
    # 去除省份后缀，只保留城市名
    city_str = clean_text(city_str)
    city_str = re.sub(r'省|市|区|县|自治区', '', city_str)
    return city_str
