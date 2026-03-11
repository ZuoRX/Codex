"""
拉勾网爬虫 (lagou.com)
- 使用Playwright + API拦截
- 拉勾网以互联网/科技职位为主，数据分析岗位质量高
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


class LagouScraper(BaseScraper):
    """拉勾网爬虫"""

    SITE_NAME = "拉勾网"
    BASE_URL = "https://www.lagou.com"

    def __init__(self):
        super().__init__()
        self.cities_config = TARGET_CITIES.get("lagou", {})
        self._api_data = []

    async def setup(self):
        await super().setup()
        await self.page.route("**/lagou.com/jobs/positionAjax.json**", self._handle_api_response)
        await self.page.route("**/lagou.com/wn/position/searchPosition**", self._handle_api_response)

    async def _handle_api_response(self, route):
        try:
            response = await route.fetch()
            body = await response.text()
            data = safe_json_loads(body)
            if data:
                items = (
                    data.get("content", {}).get("positionResult", {}).get("result", []) or
                    data.get("data", {}).get("page", {}).get("result", []) or
                    data.get("data", []) or
                    []
                )
                if items:
                    self._api_data.extend(items)
                    logger.debug(f"[{self.SITE_NAME}] API拦截 {len(items)} 条")
        except Exception as e:
            logger.debug(f"[{self.SITE_NAME}] API拦截失败: {e}")
        finally:
            await route.continue_()

    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        jobs = []
        page_num = 1
        max_pages = SCRAPE_CONFIG["max_pages_per_keyword"]

        # 先访问首页建立Cookie
        await self.safe_goto(self.BASE_URL, wait_for="domcontentloaded")
        await random_delay(2, 3)

        while page_num <= max_pages:
            self._api_data = []

            # 拉勾搜索URL
            url = (
                f"{self.BASE_URL}/wn/zhaopin"
                f"?pn={page_num}"
                f"&kd={keyword}"
                f"&city={city_code}"
                f"&cl=false"
                f"&fromSearch=true"
                f"&suginput={keyword}"
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
        jobs = []
        for item in items:
            try:
                # 薪资
                salary_str = item.get("salary", "") or item.get("salaryDesc", "") or "面议"

                title = clean_text(item.get("positionName", "") or item.get("title", ""))

                # 公司信息
                company_name = item.get("companyFullName", "") or item.get("companyShortName", "")
                company_size = item.get("companySize", "")
                company_type = item.get("financeStage", "")  # 融资阶段
                industry = item.get("industryField", "")

                # 地点
                city_name = item.get("city", "") or city
                district = item.get("district", "") or ""

                # 经验学历
                experience = item.get("workYear", "") or ""
                education = item.get("education", "") or ""

                # 描述
                desc = clean_text(item.get("positionDetail", "") or item.get("description", "") or "")

                # 时间
                publish_time = item.get("createTime", "") or item.get("refreshTime", "")
                if publish_time:
                    publish_time = str(publish_time)[:10]

                # 链接
                pos_id = item.get("positionId", "")
                job_url = f"{self.BASE_URL}/jobs/{pos_id}.html" if pos_id else ""

                # 福利
                welfare_list = item.get("positionAdvantage", "") or ""
                if isinstance(welfare_list, str):
                    welfare = welfare_list
                elif isinstance(welfare_list, list):
                    welfare = "、".join(welfare_list)
                else:
                    welfare = ""

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
                    "经验要求": parse_experience(experience),
                    "学历要求": parse_education(education),
                    "职位描述": desc[:2000],
                    "技能要求": extract_skills_from_description(desc),
                    "福利待遇": welfare,
                    "招聘人数": str(item.get("recruitmentNum", "若干") or "若干"),
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
        """DOM解析拉勾网职位列表"""
        jobs = []
        try:
            await self.page.wait_for_selector(".jobs-wrapper", timeout=15000)
        except PlaywrightTimeoutError:
            logger.warning(f"[{self.SITE_NAME}] 列表页加载超时")
            return jobs

        try:
            job_cards = await self.page.query_selector_all(".jobs-wrapper .job-item")
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
            title_el = await card.query_selector(".position-name, h3")
            title = clean_text(await title_el.inner_text()) if title_el else ""

            salary_el = await card.query_selector(".money, .salary")
            salary_str = clean_text(await salary_el.inner_text()) if salary_el else "面议"

            company_el = await card.query_selector(".company-name")
            company = clean_text(await company_el.inner_text()) if company_el else ""

            city_el = await card.query_selector(".work-location")
            city = clean_text(await city_el.inner_text()) if city_el else ""

            exp_el = await card.query_selector(".work-year")
            experience = clean_text(await exp_el.inner_text()) if exp_el else ""

            edu_el = await card.query_selector(".education")
            education = clean_text(await edu_el.inner_text()) if edu_el else ""

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
            logger.debug(f"[{self.SITE_NAME}] DOM卡片解析失败: {e}")
            return None

    async def parse_job_detail(self, job_url: str) -> Dict:
        detail = {"职位描述": "", "技能要求": ""}
        try:
            await self.safe_goto(job_url)
            await random_delay(1, 2)
            desc_el = await self.page.query_selector(".job_detail, .job-description")
            if desc_el:
                desc_text = clean_text(await desc_el.inner_text())
                detail["职位描述"] = desc_text[:2000]
                detail["技能要求"] = extract_skills_from_description(desc_text)
        except Exception:
            pass
        return detail

    async def _has_next_page(self) -> bool:
        try:
            next_btn = await self.page.query_selector(".page-next, .next-page")
            if not next_btn:
                return False
            disabled = await next_btn.get_attribute("class") or ""
            return "disabled" not in disabled
        except Exception:
            return False
