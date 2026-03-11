"""
智联招聘爬虫 (zhaopin.com)
- 使用Playwright动态加载
- API接口爬取（更稳定）
"""
import asyncio
import json
import random
from typing import List, Dict, Optional
from datetime import datetime
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from scrapers.base_scraper import BaseScraper
from utils.helpers import (
    parse_salary, clean_text, parse_experience,
    parse_education, parse_company_size, random_delay,
    human_scroll, format_datetime, safe_json_loads,
    extract_skills_from_description
)
from config import SCRAPE_CONFIG, TARGET_CITIES


class ZhaopinScraper(BaseScraper):
    """智联招聘爬虫"""

    SITE_NAME = "智联招聘"
    BASE_URL = "https://www.zhaopin.com"
    SEARCH_URL = "https://sou.zhaopin.com"

    def __init__(self):
        super().__init__()
        self.cities_config = TARGET_CITIES.get("zhaopin", {})
        self._api_data = []

    async def setup(self):
        """初始化，并监听API响应"""
        await super().setup()
        # 拦截智联招聘的搜索API响应
        await self.page.route("**/sou.zhaopin.com/jobs/searchresult/**", self._handle_api_response)
        await self.page.route("**/fe-bi-gateway.zhaopin.com/sou/**", self._handle_api_response)
        await self.page.route("**/gateway.zhaopin.com/common-job-retrieval/**", self._handle_api_response)

    async def _handle_api_response(self, route):
        """处理API响应数据"""
        try:
            response = await route.fetch()
            body = await response.text()
            data = safe_json_loads(body)
            if data and isinstance(data, dict):
                items = (
                    data.get("data", {}).get("results", []) or
                    data.get("data", {}).get("list", []) or
                    data.get("data", []) or
                    []
                )
                if items:
                    self._api_data.extend(items)
                    logger.debug(f"[{self.SITE_NAME}] API拦截到 {len(items)} 条数据")
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] API拦截处理失败: {e}")
        finally:
            await route.continue_()

    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        """爬取指定关键词和城市"""
        jobs = []
        page_num = 1
        max_pages = SCRAPE_CONFIG["max_pages_per_keyword"]

        while page_num <= max_pages:
            self._api_data = []  # 清空API缓存

            url = (
                f"{self.SEARCH_URL}/"
                f"?jl={city_code}"
                f"&kw={keyword}"
                f"&p={page_num}"
                f"&sortType=publish_time"
            )

            logger.info(f"[{self.SITE_NAME}] 爬取第{page_num}页: {keyword} - {city}")

            success = await self.safe_goto(url, wait_for="domcontentloaded")
            if not success:
                break

            if await self.check_anti_bot():
                await self.handle_anti_bot()
                continue

            await random_delay(2, 4)
            await human_scroll(self.page, times=3)

            # 先尝试从API数据解析
            if self._api_data:
                page_jobs = self._parse_api_data(self._api_data, city)
            else:
                # 回退到DOM解析
                page_jobs = await self.parse_job_list_page()

            if not page_jobs:
                logger.info(f"[{self.SITE_NAME}] 第{page_num}页无数据，停止")
                break

            # 补充城市信息
            for job in page_jobs:
                if not job.get("工作城市"):
                    job["工作城市"] = city

            jobs.extend(page_jobs)
            logger.info(f"[{self.SITE_NAME}] 第{page_num}页获取 {len(page_jobs)} 条，累计 {len(jobs)} 条")

            has_next = await self._has_next_page()
            if not has_next:
                break

            page_num += 1
            await random_delay(
                SCRAPE_CONFIG["page_delay_min"],
                SCRAPE_CONFIG["page_delay_max"]
            )

        return jobs

    def _parse_api_data(self, api_items: List[dict], city: str) -> List[Dict]:
        """从API响应数据解析职位信息"""
        jobs = []
        for item in api_items:
            try:
                # 薪资
                salary_low = item.get("salaryFrom", 0) or 0
                salary_high = item.get("salaryTo", 0) or 0
                if salary_low or salary_high:
                    salary_str = f"{salary_low}-{salary_high}K"
                else:
                    salary_str = "面议"

                # 公司信息
                company = item.get("company", {}) or {}
                company_name = company.get("name", "") or item.get("companyName", "")
                company_size = company.get("size", {})
                if isinstance(company_size, dict):
                    company_size = company_size.get("name", "")
                company_type = company.get("type", {})
                if isinstance(company_type, dict):
                    company_type = company_type.get("name", "")
                industry = company.get("industry", {})
                if isinstance(industry, dict):
                    industry = industry.get("name", "")
                elif isinstance(industry, list) and industry:
                    industry = industry[0].get("name", "") if isinstance(industry[0], dict) else str(industry[0])

                # 地点
                city_name = item.get("city", {})
                if isinstance(city_name, dict):
                    city_name = city_name.get("display", city)
                elif not city_name:
                    city_name = city

                work_area = item.get("workingCity", "") or item.get("district", "")
                if isinstance(work_area, dict):
                    work_area = work_area.get("display", "")

                # 经验学历
                exp = item.get("workingExp", {})
                if isinstance(exp, dict):
                    exp = exp.get("name", "")
                edu = item.get("education", {})
                if isinstance(edu, dict):
                    edu = edu.get("name", "")

                # 职位描述
                desc = clean_text(item.get("briefDesc", "") or item.get("jobSummary", ""))

                # 发布时间
                publish_time = item.get("publishTime", "") or item.get("lastModifyTime", "")
                if publish_time:
                    publish_time = str(publish_time)[:10]

                # 职位链接
                job_id = item.get("number", "") or item.get("jobId", "")
                job_url = f"https://jobs.zhaopin.com/{job_id}.htm" if job_id else ""

                salary_low_k = salary_low / 1000 if salary_low > 1000 else salary_low
                salary_high_k = salary_high / 1000 if salary_high > 1000 else salary_high

                jobs.append({
                    "职位名称": clean_text(item.get("name", "") or item.get("jobName", "")),
                    "薪资范围": salary_str,
                    "薪资下限_千元": salary_low_k if salary_low_k else None,
                    "薪资上限_千元": salary_high_k if salary_high_k else None,
                    "薪资单位": "千元/月",
                    "工作城市": city_name,
                    "工作区域": work_area,
                    "公司名称": company_name,
                    "公司规模": company_size,
                    "公司类型": company_type,
                    "行业类别": industry,
                    "经验要求": parse_experience(exp),
                    "学历要求": parse_education(edu),
                    "职位描述": desc[:2000],
                    "技能要求": extract_skills_from_description(desc),
                    "福利待遇": "、".join(item.get("welfare", []) or []),
                    "招聘人数": str(item.get("recruitCount", "若干") or "若干"),
                    "发布时间": publish_time,
                    "截止日期": item.get("endDate", ""),
                    "来源网站": self.SITE_NAME,
                    "职位链接": job_url,
                    "爬取时间": format_datetime(),
                })
            except Exception as e:
                logger.debug(f"[{self.SITE_NAME}] API数据解析失败: {e}")
                continue
        return jobs

    async def parse_job_list_page(self) -> List[Dict]:
        """DOM解析智联招聘职位列表页"""
        jobs = []
        try:
            await self.page.wait_for_selector(".positionResult__list", timeout=15000)
        except PlaywrightTimeoutError:
            logger.warning(f"[{self.SITE_NAME}] 列表页加载超时")
            return jobs

        try:
            job_cards = await self.page.query_selector_all(".positionResult__item")
            for card in job_cards:
                try:
                    job = await self._parse_dom_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SITE_NAME}] 解析卡片失败: {e}")
        except Exception as e:
            logger.error(f"[{self.SITE_NAME}] 解析列表页失败: {e}")

        return jobs

    async def _parse_dom_card(self, card) -> Optional[Dict]:
        """解析智联招聘DOM职位卡片"""
        try:
            title_el = await card.query_selector(".jobname__name")
            title = clean_text(await title_el.inner_text()) if title_el else ""

            salary_el = await card.query_selector(".jobinfo__salary")
            salary_str = clean_text(await salary_el.inner_text()) if salary_el else "面议"

            city_el = await card.query_selector(".jobinfo__city")
            city = clean_text(await city_el.inner_text()) if city_el else ""

            company_el = await card.query_selector(".companylist__name")
            company = clean_text(await company_el.inner_text()) if company_el else ""

            exp_el = await card.query_selector(".jobinfo__exp")
            experience = clean_text(await exp_el.inner_text()) if exp_el else ""

            edu_el = await card.query_selector(".jobinfo__edu")
            education = clean_text(await edu_el.inner_text()) if edu_el else ""

            link_el = await card.query_selector("a.jobname__name")
            href = await link_el.get_attribute("href") if link_el else ""

            salary_low, salary_high, salary_unit = parse_salary(salary_str)

            return {
                "职位名称": title,
                "薪资范围": salary_str,
                "薪资下限_千元": salary_low,
                "薪资上限_千元": salary_high,
                "薪资单位": salary_unit,
                "工作城市": city,
                "工作区域": "",
                "公司名称": company,
                "公司规模": "",
                "公司类型": "",
                "行业类别": "",
                "经验要求": parse_experience(experience),
                "学历要求": parse_education(education),
                "职位描述": "",
                "技能要求": "",
                "福利待遇": "",
                "招聘人数": "若干",
                "发布时间": "",
                "截止日期": "",
                "来源网站": self.SITE_NAME,
                "职位链接": href,
                "爬取时间": format_datetime(),
            }
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] DOM解析失败: {e}")
            return None

    async def parse_job_detail(self, job_url: str) -> Dict:
        """解析职位详情"""
        detail = {"职位描述": "", "技能要求": "", "招聘人数": "若干"}
        try:
            await self.safe_goto(job_url)
            await random_delay(1, 2)

            desc_el = await self.page.query_selector(".describtion__detail-cont")
            if desc_el:
                desc_text = clean_text(await desc_el.inner_text())
                detail["职位描述"] = desc_text[:2000]
                detail["技能要求"] = extract_skills_from_description(desc_text)
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] 详情获取失败: {e}")
        return detail

    async def _has_next_page(self) -> bool:
        """检查是否有下一页"""
        try:
            next_btn = await self.page.query_selector(".paginationCom__item--next")
            if next_btn:
                disabled = await next_btn.get_attribute("class")
                return "disable" not in (disabled or "")
            return False
        except Exception:
            return False
