"""
猎聘网爬虫 (liepin.com)
- 使用Playwright动态加载 + API拦截
- 猎聘以中高端职位为主
"""
import asyncio
import random
from typing import List, Dict, Optional
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from scrapers.base_scraper import BaseScraper
from utils.helpers import (
    parse_salary, clean_text, parse_experience,
    parse_education, random_delay, human_scroll,
    format_datetime, safe_json_loads, extract_skills_from_description
)
from config import SCRAPE_CONFIG, TARGET_CITIES


class LiepinScraper(BaseScraper):
    """猎聘网爬虫"""

    SITE_NAME = "猎聘网"
    BASE_URL = "https://www.liepin.com"

    def __init__(self):
        super().__init__()
        self.cities_config = TARGET_CITIES.get("liepin", {})
        self._api_data = []

    async def setup(self):
        await super().setup()
        await self.page.route("**/api.liepin.com/**", self._handle_api_response)
        await self.page.route("**/liepin.com/api/**", self._handle_api_response)

    async def _handle_api_response(self, route):
        try:
            response = await route.fetch()
            body = await response.text()
            data = safe_json_loads(body)
            if data:
                items = (
                    data.get("data", {}).get("jobDatas", {}).get("datas", []) or
                    data.get("data", {}).get("data", []) or
                    data.get("data", []) or
                    []
                )
                if items:
                    self._api_data.extend(items)
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] API拦截失败: {e}")
        finally:
            await route.continue_()

    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        jobs = []
        page_num = 0
        max_pages = SCRAPE_CONFIG["max_pages_per_keyword"]

        while page_num < max_pages:
            self._api_data = []

            url = (
                f"{self.BASE_URL}/zhaopin/"
                f"?key={keyword}"
                f"&city={city_code}"
                f"&curPage={page_num}"
                f"&sortFlag=15"
            )

            logger.info(f"[{self.SITE_NAME}] 爬取第{page_num+1}页: {keyword} - {city}")

            success = await self.safe_goto(url, wait_for="domcontentloaded")
            if not success:
                break

            if await self.check_anti_bot():
                await self.handle_anti_bot()
                continue

            await random_delay(2, 4)
            await human_scroll(self.page, times=3)

            if self._api_data:
                page_jobs = self._parse_api_data(self._api_data, city)
            else:
                page_jobs = await self.parse_job_list_page()

            if not page_jobs:
                logger.info(f"[{self.SITE_NAME}] 第{page_num+1}页无数据，停止")
                break

            for job in page_jobs:
                if not job.get("工作城市"):
                    job["工作城市"] = city

            jobs.extend(page_jobs)
            logger.info(f"[{self.SITE_NAME}] 第{page_num+1}页获取 {len(page_jobs)} 条，累计 {len(jobs)} 条")

            has_next = await self._has_next_page()
            if not has_next:
                break

            page_num += 1
            await random_delay(
                SCRAPE_CONFIG["page_delay_min"],
                SCRAPE_CONFIG["page_delay_max"]
            )

        return jobs

    def _parse_api_data(self, items: List[dict], city: str) -> List[Dict]:
        jobs = []
        for item in items:
            try:
                job_info = item.get("job", item)

                # 薪资
                salary_str = job_info.get("salaryDesc", "") or job_info.get("salary", "") or "面议"

                # 职位基本信息
                title = clean_text(job_info.get("title", "") or job_info.get("jobTitle", ""))

                # 公司信息
                comp_info = item.get("comp", {}) or job_info.get("company", {}) or {}
                company_name = comp_info.get("compName", "") or comp_info.get("name", "")
                company_size = comp_info.get("compScale", {})
                if isinstance(company_size, dict):
                    company_size = company_size.get("name", "")
                company_type = comp_info.get("compNature", {})
                if isinstance(company_type, dict):
                    company_type = company_type.get("name", "")
                industry = comp_info.get("industryName", "") or comp_info.get("industryCategory", "")

                # 地点
                city_name = job_info.get("city", "") or city
                if isinstance(city_name, dict):
                    city_name = city_name.get("name", city)
                district = job_info.get("district", {})
                if isinstance(district, dict):
                    district = district.get("name", "")
                elif not district:
                    district = ""

                # 经验学历
                experience = job_info.get("exp", {})
                if isinstance(experience, dict):
                    experience = experience.get("name", "")
                education = job_info.get("edu", {})
                if isinstance(education, dict):
                    education = education.get("name", "")

                # 描述
                desc = clean_text(job_info.get("description", "") or job_info.get("briefDesc", "") or "")

                # 时间
                publish_time = job_info.get("publishDate", "") or job_info.get("refreshTime", "")
                if publish_time:
                    publish_time = str(publish_time)[:10]

                # 链接
                job_id = job_info.get("jobId", "") or job_info.get("id", "")
                job_url = f"{self.BASE_URL}/job/{job_id}/" if job_id else ""

                salary_low, salary_high, salary_unit = parse_salary(salary_str)

                jobs.append({
                    "职位名称": title,
                    "薪资范围": salary_str,
                    "薪资下限_千元": salary_low,
                    "薪资上限_千元": salary_high,
                    "薪资单位": salary_unit,
                    "工作城市": city_name,
                    "工作区域": district,
                    "公司名称": company_name,
                    "公司规模": company_size,
                    "公司类型": company_type,
                    "行业类别": industry,
                    "经验要求": parse_experience(str(experience)),
                    "学历要求": parse_education(str(education)),
                    "职位描述": desc[:2000],
                    "技能要求": extract_skills_from_description(desc),
                    "福利待遇": "、".join(job_info.get("welfare", []) or []),
                    "招聘人数": "若干",
                    "发布时间": publish_time,
                    "截止日期": "",
                    "来源网站": self.SITE_NAME,
                    "职位链接": job_url,
                    "爬取时间": format_datetime(),
                })
            except Exception as e:
                logger.debug(f"[{self.SITE_NAME}] 数据解析失败: {e}")
                continue
        return jobs

    async def parse_job_list_page(self) -> List[Dict]:
        """DOM解析猎聘职位列表"""
        jobs = []
        try:
            await self.page.wait_for_selector(".job-list-container", timeout=15000)
        except PlaywrightTimeoutError:
            try:
                await self.page.wait_for_selector(".jobs-list", timeout=10000)
            except PlaywrightTimeoutError:
                logger.warning(f"[{self.SITE_NAME}] 列表页加载超时")
                return jobs

        try:
            selectors = [
                ".job-list-container .job-card",
                ".jobs-list .job-item",
                ".joblist-box li",
            ]
            job_cards = []
            for sel in selectors:
                job_cards = await self.page.query_selector_all(sel)
                if job_cards:
                    break

            for card in job_cards:
                try:
                    job = await self._parse_dom_card(card)
                    if job:
                        jobs.append(job)
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"[{self.SITE_NAME}] DOM解析失败: {e}")

        return jobs

    async def _parse_dom_card(self, card) -> Optional[Dict]:
        try:
            title_el = await card.query_selector(".job-name, .title")
            title = clean_text(await title_el.inner_text()) if title_el else ""

            salary_el = await card.query_selector(".job-salary, .salary")
            salary_str = clean_text(await salary_el.inner_text()) if salary_el else "面议"

            company_el = await card.query_selector(".comp-name, .company")
            company = clean_text(await company_el.inner_text()) if company_el else ""

            city_el = await card.query_selector(".job-city, .location")
            city = clean_text(await city_el.inner_text()) if city_el else ""

            link_el = await card.query_selector("a")
            href = await link_el.get_attribute("href") if link_el else ""
            if href and not href.startswith("http"):
                href = f"{self.BASE_URL}{href}"

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
                "经验要求": "不限",
                "学历要求": "不限",
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
            logger.debug(f"[{self.SITE_NAME}] DOM卡片解析失败: {e}")
            return None

    async def parse_job_detail(self, job_url: str) -> Dict:
        detail = {"职位描述": "", "技能要求": ""}
        try:
            await self.safe_goto(job_url)
            await random_delay(1, 2)
            desc_el = await self.page.query_selector(".job-detail-content, .content-info")
            if desc_el:
                desc_text = clean_text(await desc_el.inner_text())
                detail["职位描述"] = desc_text[:2000]
                detail["技能要求"] = extract_skills_from_description(desc_text)
        except Exception:
            pass
        return detail

    async def _has_next_page(self) -> bool:
        try:
            next_btn = await self.page.query_selector(".ant-pagination-next, .btn-next")
            if not next_btn:
                return False
            disabled = await next_btn.get_attribute("class") or ""
            return "disabled" not in disabled
        except Exception:
            return False
