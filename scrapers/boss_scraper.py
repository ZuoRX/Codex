"""
BOSS直聘爬虫 (zhipin.com)
- 使用Playwright动态加载
- 支持无需登录的列表页爬取
"""
import asyncio
import random
from typing import List, Dict, Optional
from datetime import datetime
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from scrapers.base_scraper import BaseScraper
from utils.helpers import (
    parse_salary, clean_text, parse_experience,
    parse_education, parse_company_size, random_delay,
    human_scroll, format_datetime, extract_skills_from_description
)
from config import SCRAPE_CONFIG, TARGET_CITIES


class BossScraper(BaseScraper):
    """BOSS直聘爬虫"""

    SITE_NAME = "BOSS直聘"
    BASE_URL = "https://www.zhipin.com"

    def __init__(self):
        super().__init__()
        self.cities_config = TARGET_CITIES.get("boss", {})

    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        """爬取指定关键词和城市"""
        jobs = []
        page_num = 1
        max_pages = SCRAPE_CONFIG["max_pages_per_keyword"]

        while page_num <= max_pages:
            url = (
                f"{self.BASE_URL}/web/geek/job"
                f"?query={keyword}"
                f"&city={city_code}"
                f"&page={page_num}"
                f"&salary=0"
                f"&experience=0"
                f"&degree=0"
            )

            logger.info(f"[{self.SITE_NAME}] 爬取第{page_num}页: {keyword} - {city}")

            success = await self.safe_goto(url, wait_for="domcontentloaded")
            if not success:
                break

            # 检查反爬
            if await self.check_anti_bot():
                await self.handle_anti_bot()
                continue

            await random_delay(2, 4)
            await human_scroll(self.page, times=3)

            # 解析职位列表
            page_jobs = await self.parse_job_list_page()

            if not page_jobs:
                logger.info(f"[{self.SITE_NAME}] 第{page_num}页无数据，停止")
                break

            jobs.extend(page_jobs)
            logger.info(f"[{self.SITE_NAME}] 第{page_num}页获取 {len(page_jobs)} 条，累计 {len(jobs)} 条")

            # 检查是否有下一页
            has_next = await self._has_next_page()
            if not has_next:
                break

            page_num += 1
            await random_delay(
                SCRAPE_CONFIG["page_delay_min"],
                SCRAPE_CONFIG["page_delay_max"]
            )

        return jobs

    async def parse_job_list_page(self) -> List[Dict]:
        """解析BOSS直聘职位列表页"""
        jobs = []
        try:
            # 等待职位列表加载
            await self.page.wait_for_selector(".job-list-box", timeout=15000)
        except PlaywrightTimeoutError:
            logger.warning(f"[{self.SITE_NAME}] 列表页加载超时")
            return jobs

        try:
            # 获取所有职位卡片
            job_cards = await self.page.query_selector_all(".job-list-box .job-card-wrapper")

            for card in job_cards:
                try:
                    job = await self._parse_job_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SITE_NAME}] 解析卡片失败: {e}")
                    continue

        except Exception as e:
            logger.error(f"[{self.SITE_NAME}] 解析列表页失败: {e}")

        return jobs

    async def _parse_job_card(self, card) -> Optional[Dict]:
        """解析单个职位卡片"""
        try:
            # 职位名称
            title_el = await card.query_selector(".job-name")
            title = clean_text(await title_el.inner_text()) if title_el else ""

            # 职位链接
            link_el = await card.query_selector("a.job-card-left")
            href = await link_el.get_attribute("href") if link_el else ""
            job_url = f"{self.BASE_URL}{href}" if href and not href.startswith("http") else href

            # 薪资
            salary_el = await card.query_selector(".salary")
            salary_str = clean_text(await salary_el.inner_text()) if salary_el else "面议"

            # 工作地点
            area_el = await card.query_selector(".job-area")
            area = clean_text(await area_el.inner_text()) if area_el else ""

            # 标签（经验、学历）
            tags = []
            tag_els = await card.query_selector_all(".tag-list li")
            for tag_el in tag_els:
                tags.append(clean_text(await tag_el.inner_text()))

            experience = ""
            education = ""
            for tag in tags:
                if "年" in tag or "经验" in tag or "不限" in tag:
                    experience = tag
                elif any(edu in tag for edu in ["本科", "硕士", "博士", "大专", "高中", "不限学历"]):
                    education = tag

            # 公司名称
            company_el = await card.query_selector(".company-name")
            company = clean_text(await company_el.inner_text()) if company_el else ""

            # 公司标签（规模、行业、融资）
            company_tags = []
            comp_tag_els = await card.query_selector_all(".company-tag-list li")
            for ct_el in comp_tag_els:
                company_tags.append(clean_text(await ct_el.inner_text()))

            company_size = ""
            industry = ""
            company_type = ""
            for ct in company_tags:
                if "人" in ct:
                    company_size = ct
                elif any(x in ct for x in ["上市", "外资", "合资", "国企", "民营", "初创"]):
                    company_type = ct
                else:
                    industry = ct

            # 福利
            welfare_tags = []
            welfare_els = await card.query_selector_all(".job-card-footer .tag-list li")
            for w_el in welfare_els:
                welfare_tags.append(clean_text(await w_el.inner_text()))

            salary_low, salary_high, salary_unit = parse_salary(salary_str)

            return {
                "职位名称": title,
                "薪资范围": salary_str,
                "薪资下限_千元": salary_low,
                "薪资上限_千元": salary_high,
                "薪资单位": salary_unit,
                "工作城市": area.split("·")[0] if "·" in area else area,
                "工作区域": area.split("·")[1] if "·" in area else "",
                "公司名称": company,
                "公司规模": company_size,
                "公司类型": company_type,
                "行业类别": industry,
                "经验要求": parse_experience(experience),
                "学历要求": parse_education(education),
                "职位描述": "",
                "技能要求": "",
                "福利待遇": "、".join(welfare_tags),
                "招聘人数": "若干",
                "发布时间": "",
                "截止日期": "",
                "来源网站": self.SITE_NAME,
                "职位链接": job_url,
                "爬取时间": format_datetime(),
            }
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] 解析卡片出错: {e}")
            return None

    async def parse_job_detail(self, job_url: str) -> Dict:
        """解析职位详情页（获取完整描述）"""
        detail = {"职位描述": "", "技能要求": "", "招聘人数": "若干"}
        try:
            await self.safe_goto(job_url)
            await random_delay(1, 2)

            # 职位描述
            desc_el = await self.page.query_selector(".job-detail-section")
            if desc_el:
                desc_text = clean_text(await desc_el.inner_text())
                detail["职位描述"] = desc_text[:2000]  # 限制长度
                detail["技能要求"] = extract_skills_from_description(desc_text)

            # 发布时间
            time_el = await self.page.query_selector(".job-detail-header time")
            if time_el:
                detail["发布时间"] = clean_text(await time_el.inner_text())

        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] 获取详情失败: {e}")

        return detail

    async def _has_next_page(self) -> bool:
        """检查是否有下一页"""
        try:
            next_btn = await self.page.query_selector(".options-pages a.next")
            if next_btn:
                disabled = await next_btn.get_attribute("class")
                return "disabled" not in (disabled or "")
            # 检查空结果
            empty = await self.page.query_selector(".job-empty-wrapper")
            return empty is None
        except Exception:
            return False
