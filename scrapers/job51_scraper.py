"""
前程无忧爬虫 (51job.com)
- 使用Playwright动态加载 + API拦截
"""
import asyncio
import json
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


class Job51Scraper(BaseScraper):
    """前程无忧爬虫"""

    SITE_NAME = "前程无忧"
    BASE_URL = "https://www.51job.com"
    SEARCH_URL = "https://we.51job.com/pc/search"

    def __init__(self):
        super().__init__()
        self.cities_config = TARGET_CITIES.get("job51", {})
        self._api_data = []

    async def setup(self):
        await super().setup()
        # 拦截51job搜索API
        await self.page.route("**/51job.com/Coremail/listjobs/**", self._handle_api_response)
        await self.page.route("**/search.51job.com/**", self._handle_api_response)
        await self.page.route("**/api.51job.com/**", self._handle_api_response)

    async def _handle_api_response(self, route):
        """处理API响应"""
        try:
            response = await route.fetch()
            body = await response.text()
            # 51job使用特殊编码
            try:
                data = safe_json_loads(body)
            except Exception:
                data = None

            if data:
                items = (
                    data.get("resultbody", {}).get("job", {}).get("items", []) or
                    data.get("data", {}).get("records", []) or
                    data.get("items", []) or
                    []
                )
                if items:
                    self._api_data.extend(items)
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] API拦截失败: {e}")
        finally:
            await route.continue_()

    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        """爬取指定关键词和城市"""
        jobs = []
        page_num = 1
        max_pages = SCRAPE_CONFIG["max_pages_per_keyword"]

        while page_num <= max_pages:
            self._api_data = []

            url = (
                f"{self.SEARCH_URL}"
                f"?searchType=2"
                f"&keyword={keyword}"
                f"&keywordType=0"
                f"&cityId={city_code}"
                f"&workarea={city_code}"
                f"&sortType=0"
                f"&pageNum={page_num}"
                f"&pageSize=50"
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

            # 优先使用API数据
            if self._api_data:
                page_jobs = self._parse_api_data(self._api_data, city)
            else:
                page_jobs = await self.parse_job_list_page()

            if not page_jobs:
                logger.info(f"[{self.SITE_NAME}] 第{page_num}页无数据，停止")
                break

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

    def _parse_api_data(self, items: List[dict], city: str) -> List[Dict]:
        """解析51job API数据"""
        jobs = []
        for item in items:
            try:
                # 薪资
                salary_str = item.get("providesalary_text", "") or "面议"

                # 地点
                city_name = item.get("workarea_text", "") or city
                district = item.get("attribute_text", "")

                # 公司信息
                company_name = item.get("company_name", "")
                company_size = item.get("companysize_text", "")
                company_type = item.get("companytype_text", "")
                industry = item.get("companyind_text", "")

                # 经验学历
                experience = item.get("workyear_text", "")
                education = item.get("attribute_text", "")
                # attribute_text通常包含多个属性，需要分析
                attrs = district.split(",") if district else []
                experience_found = ""
                education_found = ""
                for attr in attrs:
                    if "年" in attr or "经验" in attr:
                        experience_found = attr
                    elif any(edu in attr for edu in ["本科", "硕士", "博士", "大专"]):
                        education_found = attr

                if experience_found:
                    experience = experience_found
                if education_found:
                    education = education_found

                # 职位描述
                desc = clean_text(item.get("job_detail_info", "") or "")

                # 发布时间
                publish_time = item.get("updatedate", "") or item.get("end_date", "")

                # 职位链接
                job_url = item.get("job_href", "") or ""

                salary_low, salary_high, salary_unit = parse_salary(salary_str)

                jobs.append({
                    "职位名称": clean_text(item.get("job_name", "")),
                    "薪资范围": salary_str,
                    "薪资下限_千元": salary_low,
                    "薪资上限_千元": salary_high,
                    "薪资单位": salary_unit,
                    "工作城市": city_name,
                    "工作区域": "",
                    "公司名称": company_name,
                    "公司规模": company_size,
                    "公司类型": company_type,
                    "行业类别": industry,
                    "经验要求": parse_experience(experience),
                    "学历要求": parse_education(education),
                    "职位描述": desc[:2000],
                    "技能要求": extract_skills_from_description(desc),
                    "福利待遇": "",
                    "招聘人数": str(item.get("job_count", "若干") or "若干"),
                    "发布时间": str(publish_time)[:10] if publish_time else "",
                    "截止日期": item.get("end_date", ""),
                    "来源网站": self.SITE_NAME,
                    "职位链接": job_url,
                    "爬取时间": format_datetime(),
                })
            except Exception as e:
                logger.debug(f"[{self.SITE_NAME}] 数据解析失败: {e}")
                continue
        return jobs

    async def parse_job_list_page(self) -> List[Dict]:
        """DOM解析前程无忧职位列表"""
        jobs = []
        try:
            await self.page.wait_for_selector(".j_joblist", timeout=15000)
        except PlaywrightTimeoutError:
            logger.warning(f"[{self.SITE_NAME}] 列表页加载超时")
            return jobs

        try:
            job_cards = await self.page.query_selector_all(".e")
            if not job_cards:
                job_cards = await self.page.query_selector_all("[class*='joblist'] .j_joblist li")

            for card in job_cards:
                try:
                    job = await self._parse_dom_card(card)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.debug(f"[{self.SITE_NAME}] 解析卡片失败: {e}")
        except Exception as e:
            logger.error(f"[{self.SITE_NAME}] DOM解析失败: {e}")

        return jobs

    async def _parse_dom_card(self, card) -> Optional[Dict]:
        """解析前程无忧DOM卡片"""
        try:
            title_el = await card.query_selector(".jname")
            title = clean_text(await title_el.inner_text()) if title_el else ""

            salary_el = await card.query_selector(".sal")
            salary_str = clean_text(await salary_el.inner_text()) if salary_el else "面议"

            company_el = await card.query_selector(".cname")
            company = clean_text(await company_el.inner_text()) if company_el else ""

            area_el = await card.query_selector(".d .at")
            area = clean_text(await area_el.inner_text()) if area_el else ""

            tag_els = await card.query_selector_all(".d .t")
            tags = [clean_text(await t.inner_text()) for t in tag_els]

            link_el = await card.query_selector("a")
            href = await link_el.get_attribute("href") if link_el else ""

            salary_low, salary_high, salary_unit = parse_salary(salary_str)

            return {
                "职位名称": title,
                "薪资范围": salary_str,
                "薪资下限_千元": salary_low,
                "薪资上限_千元": salary_high,
                "薪资单位": salary_unit,
                "工作城市": area,
                "工作区域": "",
                "公司名称": company,
                "公司规模": "",
                "公司类型": "",
                "行业类别": "",
                "经验要求": parse_experience(tags[1] if len(tags) > 1 else ""),
                "学历要求": parse_education(tags[2] if len(tags) > 2 else ""),
                "职位描述": "",
                "技能要求": "",
                "福利待遇": "",
                "招聘人数": "若干",
                "发布时间": tags[0] if tags else "",
                "截止日期": "",
                "来源网站": self.SITE_NAME,
                "职位链接": href,
                "爬取时间": format_datetime(),
            }
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] DOM解析失败: {e}")
            return None

    async def parse_job_detail(self, job_url: str) -> Dict:
        detail = {"职位描述": "", "技能要求": ""}
        try:
            await self.safe_goto(job_url)
            await random_delay(1, 2)
            desc_el = await self.page.query_selector(".job_bt div")
            if desc_el:
                desc_text = clean_text(await desc_el.inner_text())
                detail["职位描述"] = desc_text[:2000]
                detail["技能要求"] = extract_skills_from_description(desc_text)
        except Exception:
            pass
        return detail

    async def _has_next_page(self) -> bool:
        try:
            next_btn = await self.page.query_selector(".p_nxt")
            if not next_btn:
                return False
            disabled = await next_btn.get_attribute("class")
            return "disabled" not in (disabled or "")
        except Exception:
            return False
