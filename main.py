"""
招聘数据爬取主程序
目标：爬取中国主要招聘网站2025-2026年数据分析相关岗位信息，总量10000+条

支持的网站:
- BOSS直聘 (zhipin.com)
- 智联招聘 (zhaopin.com)
- 前程无忧 (51job.com)
- 猎聘网 (liepin.com)
- 拉勾网 (lagou.com)

使用方法:
    python main.py                          # 爬取所有网站
    python main.py --sites boss zhaopin     # 只爬取指定网站
    python main.py --process-only           # 只处理已有原始数据
    python main.py --test                   # 测试模式(少量数据)
"""

import asyncio
import argparse
import json
import os
import sys
from datetime import datetime
from typing import List, Dict
from loguru import logger

from config import SEARCH_KEYWORDS, TARGET_CITIES, SCRAPE_CONFIG, OUTPUT_CONFIG
from data_processor import DataProcessor


def setup_logging():
    """配置日志"""
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
    logger.add(log_file, level="DEBUG", encoding="utf-8", rotation="100 MB")
    logger.info(f"日志文件: {log_file}")


async def run_scraper(scraper_class, keywords: List[str], cities: Dict[str, str],
                      site_name: str) -> List[Dict]:
    """运行单个爬虫"""
    logger.info(f"\n{'='*60}")
    logger.info(f"开始爬取: {site_name}")
    logger.info(f"关键词: {keywords}")
    logger.info(f"城市数量: {len(cities)}")
    logger.info(f"{'='*60}")

    all_jobs = []
    async with scraper_class() as scraper:
        for keyword in keywords:
            if len(all_jobs) >= SCRAPE_CONFIG["target_total"] // 5:
                logger.info(f"[{site_name}] 单站数据量已足够，停止")
                break
            for city_name, city_code in cities.items():
                try:
                    logger.info(f"[{site_name}] 爬取: {keyword} - {city_name}")
                    jobs = await scraper.scrape_keyword(keyword, city_name, city_code)
                    all_jobs.extend(jobs)
                    logger.info(f"[{site_name}] 累计: {len(all_jobs)} 条")

                    # 定期保存
                    if len(all_jobs) >= SCRAPE_CONFIG["save_interval"]:
                        scraper.jobs = all_jobs
                        scraper.save_raw_data(keyword=keyword)

                except Exception as e:
                    logger.error(f"[{site_name}] 爬取 {keyword}/{city_name} 失败: {e}")
                    continue

        # 保存最终数据
        if all_jobs:
            scraper.jobs = all_jobs
            saved_path = scraper.save_raw_data(keyword="final")
            logger.info(f"[{site_name}] 最终保存: {len(all_jobs)} 条 -> {saved_path}")

    return all_jobs


async def scrape_all_sites(sites: List[str], test_mode: bool = False) -> List[Dict]:
    """并发爬取所有网站"""

    # 测试模式使用较少的关键词和城市
    if test_mode:
        keywords = ["数据分析"]
        cities_subset = {"北京": "101010100", "上海": "101020100", "深圳": "101280600"}
        SCRAPE_CONFIG["max_pages_per_keyword"] = 2
        logger.info("测试模式: 仅爬取1个关键词 x 3个城市 x 2页")
    else:
        keywords = SEARCH_KEYWORDS
        cities_subset = None

    all_jobs = []
    tasks = []

    # 各站点爬虫配置
    site_configs = {
        "boss": {
            "module": "scrapers.boss_scraper",
            "class": "BossScraper",
            "name": "BOSS直聘",
            "cities_key": "boss",
        },
        "zhaopin": {
            "module": "scrapers.zhaopin_scraper",
            "class": "ZhaopinScraper",
            "name": "智联招聘",
            "cities_key": "zhaopin",
        },
        "job51": {
            "module": "scrapers.job51_scraper",
            "class": "Job51Scraper",
            "name": "前程无忧",
            "cities_key": "job51",
        },
        "liepin": {
            "module": "scrapers.liepin_scraper",
            "class": "LiepinScraper",
            "name": "猎聘网",
            "cities_key": "liepin",
        },
        "lagou": {
            "module": "scrapers.lagou_scraper",
            "class": "LagouScraper",
            "name": "拉勾网",
            "cities_key": "lagou",
        },
    }

    # 过滤要爬取的站点
    active_sites = {k: v for k, v in site_configs.items() if k in sites}

    if not active_sites:
        logger.error(f"未找到有效的站点配置！可选: {list(site_configs.keys())}")
        return []

    # 顺序爬取（避免被检测为机器人）
    for site_key, config in active_sites.items():
        try:
            # 动态导入
            import importlib
            module = importlib.import_module(config["module"])
            scraper_class = getattr(module, config["class"])

            # 获取城市配置
            cities = cities_subset or TARGET_CITIES.get(config["cities_key"], {})

            site_jobs = await run_scraper(
                scraper_class,
                keywords,
                cities,
                config["name"]
            )
            all_jobs.extend(site_jobs)
            logger.info(f"[{config['name']}] 完成，获取 {len(site_jobs)} 条数据")
            logger.info(f"所有站点累计: {len(all_jobs)} 条数据")

        except Exception as e:
            logger.error(f"[{config['name']}] 爬取失败: {e}", exc_info=True)
            continue

    return all_jobs


def process_and_export(all_jobs: List[Dict] = None, output_path: str = None) -> str:
    """处理数据并导出Excel"""
    processor = DataProcessor()

    if all_jobs:
        processor.load_data_from_list(all_jobs)
    else:
        count = processor.load_raw_data()
        if count == 0:
            logger.error("没有找到任何数据！请先运行爬虫。")
            return ""

    logger.info("开始数据清洗和处理...")
    df = processor.process()

    if df.empty:
        logger.error("处理后数据为空！")
        return ""

    # 生成报告
    report = processor.generate_report()
    logger.info("\n" + "="*50)
    logger.info("数据汇总报告:")
    logger.info(f"  总数据量: {report.get('总数据量', 0)} 条")
    logger.info(f"  覆盖城市: {len(report.get('城市分布Top10', {}))}+ 个")
    logger.info(f"  平均薪资: {report.get('平均薪资(K)', 'N/A')} K/月")
    logger.info(f"  有效薪资: {report.get('有效薪资数量', 0)} 条")
    logger.info("\n  来源网站:")
    for site, count in report.get("来源网站", {}).items():
        logger.info(f"    {site}: {count} 条")
    logger.info(f"\n  城市分布Top10:")
    for city, count in report.get("城市分布Top10", {}).items():
        logger.info(f"    {city}: {count} 条")
    logger.info("="*50)

    # 导出Excel
    excel_path = processor.export_excel(output_path)
    logger.info(f"\n✅ Excel导出成功: {excel_path}")
    return excel_path


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="招聘数据爬取系统 - 数据分析岗位",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                          # 爬取所有网站
  python main.py --sites boss             # 只爬取BOSS直聘
  python main.py --sites boss zhaopin     # 爬取BOSS直聘和智联招聘
  python main.py --process-only           # 只处理已有数据
  python main.py --test                   # 测试模式
        """
    )
    parser.add_argument(
        "--sites",
        nargs="+",
        choices=["boss", "zhaopin", "job51", "liepin", "lagou"],
        default=["boss", "zhaopin", "job51", "liepin", "lagou"],
        help="要爬取的网站（默认全部）"
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="只处理已有原始数据，不重新爬取"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="测试模式（少量数据）"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Excel输出路径"
    )
    parser.add_argument(
        "--headless",
        type=bool,
        default=True,
        help="是否无头模式（默认True）"
    )
    return parser.parse_args()


async def main():
    setup_logging()
    args = parse_args()

    # 更新配置
    SCRAPE_CONFIG["headless"] = args.headless

    logger.info("="*60)
    logger.info("招聘数据爬取系统启动")
    logger.info(f"目标: 数据分析相关岗位 2025-2026年")
    logger.info(f"目标数量: {SCRAPE_CONFIG['target_total']} 条")
    logger.info(f"爬取网站: {args.sites}")
    logger.info("="*60)

    if args.process_only:
        logger.info("仅处理模式：跳过爬取，直接处理现有数据")
        excel_path = process_and_export(output_path=args.output)
    else:
        start_time = datetime.now()

        # 执行爬取
        all_jobs = await scrape_all_sites(args.sites, test_mode=args.test)

        elapsed = (datetime.now() - start_time).seconds
        logger.info(f"\n爬取完成！共获取 {len(all_jobs)} 条数据，耗时 {elapsed//60}分{elapsed%60}秒")

        if not all_jobs:
            logger.warning("未获取到任何数据！")
            return

        # 保存合并的原始数据
        os.makedirs(OUTPUT_CONFIG["raw_data_dir"], exist_ok=True)
        merged_path = os.path.join(
            OUTPUT_CONFIG["raw_data_dir"],
            f"merged_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        logger.info(f"合并原始数据保存至: {merged_path}")

        # 处理并导出
        excel_path = process_and_export(all_jobs=all_jobs, output_path=args.output)

    if excel_path:
        logger.info(f"\n🎉 任务完成！")
        logger.info(f"   Excel文件: {excel_path}")


if __name__ == "__main__":
    asyncio.run(main())
