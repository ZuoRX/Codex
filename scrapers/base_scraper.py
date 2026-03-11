"""
爬虫基类 - 提供通用的浏览器控制和反检测功能
"""
import asyncio
import random
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from loguru import logger
from utils.stealth_scripts import STEALTH_SCRIPT
from utils.helpers import random_delay, human_scroll, human_mouse_move, format_datetime
from config import BROWSER_CONFIG, SCRAPE_CONFIG, OUTPUT_CONFIG


class BaseScraper(ABC):
    """招聘网站爬虫基类"""

    SITE_NAME = "base"
    BASE_URL = ""

    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.jobs: List[Dict] = []
        self.scraped_count = 0
        self.error_count = 0
        self._user_agent = random.choice(BROWSER_CONFIG["user_agents"])

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.teardown()

    async def setup(self):
        """初始化浏览器"""
        logger.info(f"[{self.SITE_NAME}] 正在启动浏览器...")
        self.playwright = await async_playwright().start()

        launch_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--lang=zh-CN,zh",
        ]

        # 检测本地已有的Chromium路径（按优先级）
        chromium_paths = [
            "/root/.cache/ms-playwright/chromium-1194/chrome-linux/chrome",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
        ]
        executable_path = None
        for path in chromium_paths:
            if os.path.exists(path):
                executable_path = path
                logger.info(f"[{self.SITE_NAME}] 使用本地Chromium: {path}")
                break

        launch_kwargs = {
            "headless": SCRAPE_CONFIG["headless"],
            "args": launch_args,
        }
        if executable_path:
            launch_kwargs["executable_path"] = executable_path
        else:
            launch_kwargs["channel"] = "chromium"

        self.browser = await self.playwright.chromium.launch(**launch_kwargs)

        self.context = await self.browser.new_context(
            viewport=BROWSER_CONFIG["viewport"],
            user_agent=self._user_agent,
            locale=BROWSER_CONFIG["locale"],
            timezone_id=BROWSER_CONFIG["timezone_id"],
            extra_http_headers={
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            },
        )

        # 注入反检测脚本
        await self.context.add_init_script(STEALTH_SCRIPT)

        self.page = await self.context.new_page()

        # 阻止不必要的资源加载（加速爬取）
        await self.page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf}", lambda route: route.abort())

        logger.info(f"[{self.SITE_NAME}] 浏览器启动成功，User-Agent: {self._user_agent[:50]}...")

    async def teardown(self):
        """关闭浏览器"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info(f"[{self.SITE_NAME}] 浏览器已关闭")
        except Exception as e:
            logger.error(f"[{self.SITE_NAME}] 关闭浏览器时出错: {e}")

    async def safe_goto(self, url: str, wait_for: str = "networkidle") -> bool:
        """安全跳转页面，带重试机制"""
        for attempt in range(SCRAPE_CONFIG["max_retries"]):
            try:
                await self.page.goto(
                    url,
                    timeout=SCRAPE_CONFIG["timeout"],
                    wait_until=wait_for
                )
                await random_delay(1, 2)
                return True
            except Exception as e:
                logger.warning(f"[{self.SITE_NAME}] 第{attempt+1}次加载页面失败: {url} - {e}")
                if attempt < SCRAPE_CONFIG["max_retries"] - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    self.error_count += 1
                    return False

    async def check_anti_bot(self) -> bool:
        """检查是否触发反爬机制"""
        try:
            title = await self.page.title()
            url = self.page.url
            # 常见反爬页面特征
            anti_patterns = [
                "验证", "verify", "captcha", "robot", "安全验证",
                "访问限制", "forbidden", "403", "blocked"
            ]
            for pattern in anti_patterns:
                if pattern.lower() in title.lower() or pattern.lower() in url.lower():
                    logger.warning(f"[{self.SITE_NAME}] 检测到反爬机制: {title}")
                    return True
            return False
        except Exception:
            return False

    async def handle_anti_bot(self):
        """处理反爬机制"""
        logger.warning(f"[{self.SITE_NAME}] 触发反爬，等待30秒后重试...")
        await asyncio.sleep(30)
        # 刷新用户代理
        self._user_agent = random.choice(BROWSER_CONFIG["user_agents"])
        await self.context.set_extra_http_headers({
            "User-Agent": self._user_agent,
        })

    def save_raw_data(self, keyword: str = ""):
        """保存原始数据到JSON文件"""
        os.makedirs(OUTPUT_CONFIG["raw_data_dir"], exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{OUTPUT_CONFIG['raw_filename_prefix']}_{self.SITE_NAME}_{keyword}_{timestamp}.json"
        filepath = os.path.join(OUTPUT_CONFIG["raw_data_dir"], filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"[{self.SITE_NAME}] 保存 {len(self.jobs)} 条数据到 {filepath}")
        return filepath

    @abstractmethod
    async def scrape_keyword(self, keyword: str, city: str, city_code: str) -> List[Dict]:
        """爬取指定关键词和城市的职位"""
        pass

    @abstractmethod
    async def parse_job_list_page(self) -> List[Dict]:
        """解析职位列表页"""
        pass

    @abstractmethod
    async def parse_job_detail(self, job_url: str) -> Dict:
        """解析职位详情页"""
        pass

    async def scrape_all(self, keywords: List[str], cities: Dict[str, str]) -> List[Dict]:
        """爬取所有关键词和城市的职位"""
        all_jobs = []
        for keyword in keywords:
            for city_name, city_code in cities.items():
                if len(all_jobs) >= SCRAPE_CONFIG["target_total"]:
                    logger.info(f"[{self.SITE_NAME}] 已达到目标数量 {SCRAPE_CONFIG['target_total']}，停止爬取")
                    break
                logger.info(f"[{self.SITE_NAME}] 开始爬取: 关键词={keyword}, 城市={city_name}")
                try:
                    jobs = await self.scrape_keyword(keyword, city_name, city_code)
                    all_jobs.extend(jobs)
                    logger.info(f"[{self.SITE_NAME}] 当前总计: {len(all_jobs)} 条")
                    await random_delay(3, 6)
                except Exception as e:
                    logger.error(f"[{self.SITE_NAME}] 爬取 {keyword}/{city_name} 时出错: {e}")
            if len(all_jobs) >= SCRAPE_CONFIG["target_total"]:
                break

        self.jobs = all_jobs
        return all_jobs
