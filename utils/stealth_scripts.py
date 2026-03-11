"""
浏览器反检测脚本 - 注入JavaScript绕过反爬机制
"""

# 核心反检测脚本
STEALTH_SCRIPT = """
// 隐藏 webdriver 标识
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined,
    configurable: true
});

// 伪造 plugins
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        return [
            {filename: 'internal-nacl-plugin', description: 'Native Client'},
            {filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: 'Chrome PDF Viewer'},
            {filename: 'internal-pdf-viewer', description: 'Chromium PDF Viewer'},
        ];
    },
    configurable: true
});

// 伪造语言设置
Object.defineProperty(navigator, 'languages', {
    get: () => ['zh-CN', 'zh', 'en-US', 'en'],
    configurable: true
});

// 移除 automation 相关属性
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

// 伪造 Chrome 对象
window.chrome = {
    runtime: {},
    loadTimes: function() {},
    csi: function() {},
    app: {},
};

// 修复 permissions API
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
        Promise.resolve({state: Notification.permission}) :
        originalQuery(parameters)
);

// 伪造 WebGL 指纹
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(parameter) {
    if (parameter === 37445) {
        return 'Intel Open Source Technology Center';
    }
    if (parameter === 37446) {
        return 'Mesa DRI Intel(R) Ivybridge Mobile';
    }
    return getParameter.call(this, parameter);
};

// 伪造 screen 分辨率
Object.defineProperty(screen, 'width', {get: () => 1920});
Object.defineProperty(screen, 'height', {get: () => 1080});
Object.defineProperty(screen, 'availWidth', {get: () => 1920});
Object.defineProperty(screen, 'availHeight', {get: () => 1040});
Object.defineProperty(screen, 'colorDepth', {get: () => 24});
Object.defineProperty(screen, 'pixelDepth', {get: () => 24});

// 伪造 platform
Object.defineProperty(navigator, 'platform', {
    get: () => 'Win32',
    configurable: true
});

// 伪造 hardwareConcurrency
Object.defineProperty(navigator, 'hardwareConcurrency', {
    get: () => 8,
    configurable: true
});

// 伪造 deviceMemory
Object.defineProperty(navigator, 'deviceMemory', {
    get: () => 8,
    configurable: true
});

console.log('[Stealth] Anti-detection scripts loaded');
"""

# BOSS直聘特定绕过脚本
BOSS_STEALTH_SCRIPT = """
// 绕过BOSS直聘的反爬检测
window._bzsec = undefined;
Object.defineProperty(window, '__bz_secdance', {
    value: undefined,
    writable: true
});
"""

# 智联招聘特定绕过脚本
ZHAOPIN_STEALTH_SCRIPT = """
// 绕过智联招聘的检测
window._smartbanner = undefined;
"""
