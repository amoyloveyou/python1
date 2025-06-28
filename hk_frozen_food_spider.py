# ======= 超级环境自愈修复区块 =======
import sys, os, subprocess, time, shutil, site, traceback, socket, random
import logging
import argparse
from datetime import datetime
from typing import Optional
import openai

# ========== AI平台Key与镜像配置 ==========
# 推荐注册 https://api2d.com/ 免费获取API Key，充值便宜，国内可用
# 你的API2D Key填在下方
API2D_KEY = "fk233800-jvYWBK88n2msM3Xo9giGeZJ9mox2Aw9f"  # <<< 用户已填写Key
openai.api_key = API2D_KEY
openai.api_base = "https://openai.api2d.net/v1"

# 健壮导入依赖
try:
    import asyncio
    import aiohttp
    from playwright.async_api import async_playwright
    import redis
    import re
    import pandas as pd
    from lxml import etree
except ImportError as e:
    print(f"请先安装依赖: pip install aiohttp playwright redis lxml pandas openpyxl\n详细错误: {e}")
    sys.exit(1)

# 代理池配置（如无科学上网，自动本地直连）
def get_proxy(region=None):
    return None  # 生产环境可对接真实代理池

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
]
def get_random_user_agent():
    return random.choice(USER_AGENTS)

# Redis配置与健壮去重
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
try:
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    r.ping()
    REDIS_OK = True
except Exception:
    print("[警告] Redis连接失败，自动切换为本地去重模式。建议启动Redis以获得更高性能！")
    r = None
    REDIS_OK = False

class SafeRedisSet:
    def __init__(self, key: str, redis_conn: Optional[redis.StrictRedis] = None):
        self.key = key
        self.redis = redis_conn
        self.local_set = set()
        self.use_local = not (self.redis and REDIS_OK)
    def check_and_add(self, value: str) -> bool:
        if not value:
            return False
        if self.use_local:
            if value in self.local_set:
                return False
            self.local_set.add(value)
            return True
        else:
            assert self.redis is not None, "Redis连接不可用"
            if self.redis.sismember(self.key, value):
                return False
            self.redis.sadd(self.key, value)
            return True

# 字段映射规则
FIELD_MAP = {
    'hkfoodbuy': {
        'company': '//div[@class="company-name"]/a/text()',
        'contact': '//div[@class="contact-person"]/text()',
        'phone': '//div[@class="contact-phone"]/text()',
        'whatsapp': '//div[@class="contact-whatsapp"]/a/@href',
        'product': '//div[@class="business-scope"]/text()'
    },
}

def validate_phone(phone):
    return re.match(r'^(852|853)?[0-9]{7,8}$', phone)

def extract_whatsapp_number(href):
    match = re.search(r'(852|853)\d{7,8}', href or '')
    return match.group(0) if match else ''

async def validate_whatsapp(phone):
    # 只做格式校验，不请求外部接口
    return bool(re.match(r'^(852|853)?[0-9]{7,8}$', phone))

def extract_fields(html, field_map, target_name=None):
    tree = etree.HTML(html)
    if tree is None:
        return {}
    result = {}
    for k, xpath in field_map.items():
        v = tree.xpath(xpath)
        if isinstance(v, list):
            v = v[0] if v else ''
        result[k] = v.strip() if isinstance(v, str) else v
    if 'whatsapp' in result:
        result['whatsapp'] = extract_whatsapp_number(result['whatsapp'])
    # 如果主要字段为空，尝试AI兜底
    if not result.get('company') and target_name:
        # 字段描述自动生成
        field_desc = '\n'.join([f'{k}: {v}' for k, v in field_map.items()])
        if openai.api_key:
            try:
                ai_result = asyncio.run(ai_extract_fields(html, target_name, field_desc))
                result.update(ai_result)
            except Exception as e:
                print(f"[AI字段提取异常] {e}")
        else:
            print("[AI字段提取跳过] 未配置API2D Key，AI兜底功能不可用。请注册 https://api2d.com/ 获取Key 并填写到 API2D_KEY 变量！")
    return result

# ================== 异步采集主流程 ==================
async def fetch_with_playwright(url, proxy=None, user_agent=None, pagination=None):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
            context = await browser.new_context(user_agent=user_agent or get_random_user_agent())
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            if pagination and pagination.get("type") == "scroll":
                for _ in range(10):
                    await page.evaluate(pagination["trigger"])
                    await asyncio.sleep(random.uniform(1, 2))
            await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            await page.mouse.click(random.randint(100, 500), random.randint(100, 500))
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        print(f"[Playwright采集异常] {url} | {e}")
        return ''

async def fetch_with_aiohttp(url, proxy=None, user_agent=None):
    headers = {'User-Agent': user_agent or get_random_user_agent()}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, proxy=proxy, timeout=30) as resp:
                return await resp.text()
    except Exception as e:
        print(f"[aiohttp采集异常] {url} | {e}")
        return ''

async def async_crawl(urls, field_map, use_playwright=False, concurrency=20, sleep_range=(1,2), safe_set=None):
    sem = asyncio.Semaphore(concurrency)
    results = []
    if safe_set is None or not hasattr(safe_set, 'check_and_add'):
        raise ValueError("safe_set 参数必须为 SafeRedisSet 实例且包含 check_and_add 方法")
    async def crawl_one(url):
        async with sem:
            proxy = get_proxy()
            user_agent = get_random_user_agent()
            await asyncio.sleep(random.uniform(*sleep_range))
            try:
                if use_playwright:
                    html = await fetch_with_playwright(url, proxy, user_agent)
                else:
                    html = await fetch_with_aiohttp(url, proxy, user_agent)
                fields = extract_fields(html, field_map)
                # Redis去重
                if not fields.get('company') or not safe_set.check_and_add(fields['company']):
                    return None
                if fields.get('phone') and not validate_phone(fields['phone']):
                    fields['phone'] = ''
                if fields.get('whatsapp'):
                    is_valid = await validate_whatsapp(fields['whatsapp'])
                    if not is_valid:
                        fields['whatsapp'] = ''
                return fields
            except Exception as e:
                logging.error(f"[异步采集异常] {url} | {e}\n{traceback.format_exc()}")
                return None
    tasks = [crawl_one(url) for url in urls]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            results.append(result)
    return results
# ================== END 异步采集主流程 ==================

# 多目标网站配置
TARGETS = [
    {
        "name": "HKTVmall餐饮供应商目录",
        "key": "hktvmall",
        "url": "https://www.hktvmall.com/category/frozen-food",
        "type": "dynamic",
        "xpath": {
            "company": "//div[@class='merchant-card']/h3/a/text()",
            "contact": "//div[contains(@class,'contact-section')]/p[1]/text()",
            "phone": "substring-before(//span[contains(text(),'Tel:')]/following-sibling::text(), 'WhatsApp')",
            "whatsapp": "//a[contains(@href,'wa.me')]/@href"
        },
        "pagination": {
            "type": "scroll",
            "trigger": "window.scrollTo(0, document.body.scrollHeight)"
        }
    },
    {
        "name": "FoodWise澳门酒店供应商",
        "key": "foodwise",
        "url": "https://www.foodwise.com.mo/suppliers",
        "type": "static",
        "xpath": {
            "company": "//table[@id='supplier-list']//td[1]/text()",
            "product_type": "//td[2]/text()",
            "contact": "//td[3]/text()"
        },
        "captcha": {
            "type": "image",
            "solver": "anti-captcha"
        }
    }
]

# 黑名单公司关键词
BLACKLIST = ['贸易公司', 'Trading', '代理']

# 数据清洗与合规
import re
def clean_data(record):
    # 过滤非港澳联系方式
    if not record.get('phone', '').startswith(('852', '853')):
        return None
    # 排除中介/非批发商
    if any(keyword in record.get('company', '') for keyword in BLACKLIST):
        return None
    # 标准化WhatsApp号
    if 'whatsapp' in record:
        record['whatsapp'] = re.sub(r'[^0-9]', '', record['whatsapp'])[-8:]
    # 合规：不采集身份证、住址等
    for key in list(record.keys()):
        if key in ['身份证', '住址', 'address', 'id_card']:
            del record[key]
    return record

# 动态采集支持滚动、等待元素
async def fetch_dynamic(url, xpath, pagination=None, wait_for=None, proxy=None, user_agent=None):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
            context = await browser.new_context(user_agent=user_agent or get_random_user_agent())
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            # 滚动加载
            if pagination and pagination.get("type") == "scroll":
                for _ in range(10):
                    await page.evaluate(pagination["trigger"])
                    await asyncio.sleep(random.uniform(1, 2))
            if wait_for:
                await page.wait_for_selector(wait_for, timeout=10000)
            # 行为模拟
            await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            await page.mouse.click(random.randint(100, 500), random.randint(100, 500))
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        print(f"[Playwright采集异常] {url} | {e}")
        return ''

# 主采集流程，支持多目标
async def crawl_target(target, safe_set, concurrency=10):
    url = target['url']
    xpath = target['xpath']
    proxy = get_proxy(region='hk' if 'HK' in target['name'] else 'mo')
    user_agent = get_random_user_agent()
    html = ''
    # 优先用Playwright，失败自动降级为aiohttp
    if target['type'] == 'dynamic':
        for try_num in range(3):
            html = await fetch_with_playwright(url, proxy=proxy, user_agent=user_agent, pagination=target.get('pagination'))
            if html:
                break
            print(f'第{try_num+1}次采集失败，尝试用aiohttp采集: {url}')
            html = await fetch_with_aiohttp(url, proxy=proxy, user_agent=user_agent)
    else:
        html = await fetch_with_aiohttp(url, proxy=proxy, user_agent=user_agent)
    # 解析字段
    fields = extract_fields(html, xpath, target_name=target['name'])
    if not fields or not fields.get('company'):
        print(f"[警告] 采集失败或页面结构变更: {url}")
        return None
    record = clean_data(fields)
    if record and safe_set.check_and_add(record.get('company', '')):
        print(f"[采集成功] {record.get('company', '')}")
        return record
    return None

async def ai_extract_fields(html, target_name, field_desc):
    """
    用大模型自动提取字段，field_desc为字段说明dict
    """
    prompt = f"""
你是一名网页信息抽取专家。请从以下HTML内容中，提取出如下字段：
{field_desc}
HTML内容如下：
{html[:4000]}  # 控制长度，防止超长
请以JSON格式返回结果。
"""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # 或gpt-4
            messages=[{"role": "user", "content": prompt}],
            max_tokens=512,
            temperature=0.2,
        )
        import json
        content = response['choices'][0]['message']['content']
        return json.loads(content)
    except Exception as e:
        print(f'[AI字段提取异常] {e}')
        return {}

def ai_judge_company(company_name):
    prompt = f"请判断公司名\"{company_name}\"是否为真实的食品供应商，不是则返回0，是则返回1。"
    # 调用openai接口，略

# ========== 主入口 ==========
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', type=str, help='指定采集目标名（如hktvmall/foodwise），不指定则采集全部')
    parser.add_argument('--concurrency', type=int, default=10, help='并发数')
    args = parser.parse_args()
    concurrency = args.concurrency
    safe_set: SafeRedisSet = SafeRedisSet('company_names', r)
    results = []
    if not API2D_KEY:
        print("[提示] 你未填写API2D Key，AI字段提取功能不可用。可前往 https://api2d.com/ 免费注册获取Key，充值便宜，国内可用！")
    async def run_all():
        selected = [t for t in TARGETS if (not args.target or t['key'] == args.target)]
        for target in selected:
            print(f"[采集] {target['name']} ...")
            try:
                record = await crawl_target(target, safe_set, concurrency=concurrency)
                if record:
                    results.append(record)
            except Exception as e:
                logging.error(f"[采集目标异常] {target['name']} | {e}\n{traceback.format_exc()}")
    try:
        asyncio.run(run_all())
        if results:
            df = pd.DataFrame(results)
            filename = f"采集结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False)
            print(f"[导出] 数据已保存到 {filename}")
        else:
            print("[警告] 未采集到有效数据！")
    except Exception as e:
        logging.critical(f"[致命错误] 采集主流程异常: {e}\n{traceback.format_exc()}")
        print(f"[致命错误] 采集主流程异常: {e}")

