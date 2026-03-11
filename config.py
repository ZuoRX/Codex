"""
招聘数据爬取系统配置文件
"""

# 搜索关键词 - 数据分析相关职位
SEARCH_KEYWORDS = [
    "数据分析",
    "数据分析师",
    "数据运营",
    "商业分析",
    "BI分析",
    "业务分析",
    "数据挖掘",
]

# 目标城市（全国主要城市）
TARGET_CITIES = {
    "boss": {
        "全国": "100010000",
        "北京": "101010100",
        "上海": "101020100",
        "深圳": "101280600",
        "广州": "101280100",
        "杭州": "101210100",
        "成都": "101270100",
        "武汉": "101200100",
        "南京": "101190100",
        "西安": "101110100",
        "苏州": "101190400",
        "重庆": "101040100",
        "天津": "101030100",
        "合肥": "101220100",
        "郑州": "101180100",
    },
    "zhaopin": {
        "全国": "0",
        "北京": "530",
        "上海": "538",
        "深圳": "765",
        "广州": "763",
        "杭州": "653",
        "成都": "801",
        "武汉": "736",
        "南京": "635",
        "西安": "854",
        "苏州": "636",
        "重庆": "796",
        "天津": "531",
    },
    "job51": {
        "全国": "",
        "北京": "010000",
        "上海": "020000",
        "深圳": "040200",
        "广州": "040100",
        "杭州": "080200",
        "成都": "090200",
        "武汉": "180200",
        "南京": "070200",
        "西安": "200200",
        "苏州": "070300",
        "重庆": "060000",
        "天津": "030000",
    },
    "liepin": {
        "全国": "",
        "北京": "beijing",
        "上海": "shanghai",
        "深圳": "shenzhen",
        "广州": "guangzhou",
        "杭州": "hangzhou",
        "成都": "chengdu",
        "武汉": "wuhan",
        "南京": "nanjing",
        "西安": "xian",
    },
    "lagou": {
        "全国": "",
        "北京": "北京",
        "上海": "上海",
        "深圳": "深圳",
        "广州": "广州",
        "杭州": "杭州",
        "成都": "成都",
        "武汉": "武汉",
        "南京": "南京",
        "西安": "西安",
    },
}

# 爬取配置
SCRAPE_CONFIG = {
    "max_pages_per_keyword": 50,       # 每个关键词最大页数
    "page_delay_min": 2,               # 页面间最小延迟（秒）
    "page_delay_max": 5,               # 页面间最大延迟（秒）
    "request_delay_min": 1,            # 请求间最小延迟（秒）
    "request_delay_max": 3,            # 请求间最大延迟（秒）
    "max_retries": 3,                  # 最大重试次数
    "timeout": 30000,                  # 页面加载超时（毫秒）
    "headless": True,                  # 是否无头模式（False可见浏览器）
    "save_interval": 100,              # 每爬取N条数据保存一次
    "target_total": 10000,             # 目标总数据量
}

# 输出配置
OUTPUT_CONFIG = {
    "output_dir": "output",
    "raw_data_dir": "data/raw",
    "excel_filename": "数据分析岗位招聘数据_2025_2026.xlsx",
    "raw_filename_prefix": "raw_jobs",
    "encoding": "utf-8",
}

# 浏览器配置
BROWSER_CONFIG = {
    "viewport": {"width": 1920, "height": 1080},
    "locale": "zh-CN",
    "timezone_id": "Asia/Shanghai",
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ],
}

# 数据字段映射（标准字段）
DATA_FIELDS = [
    "职位名称",
    "薪资范围",
    "薪资下限_千元",
    "薪资上限_千元",
    "薪资单位",
    "工作城市",
    "工作区域",
    "公司名称",
    "公司规模",
    "公司类型",
    "行业类别",
    "经验要求",
    "学历要求",
    "职位描述",
    "技能要求",
    "福利待遇",
    "招聘人数",
    "发布时间",
    "截止日期",
    "来源网站",
    "职位链接",
    "爬取时间",
]
