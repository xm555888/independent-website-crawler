import os
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, BrowserConfig, CrawlerRunConfig,SemaphoreDispatcher,RateLimiter,CrawlerMonitor,DisplayMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter, LLMContentFilter
from crawl4ai.extraction_strategy import LLMExtractionStrategy
import pandas as pd
import hashlib
import json
import time

def ensure_url_format(url):
    """确保URL格式正确"""
    if not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url

def generate_safe_filename(url):
    """生成安全的文件名，避免路径过长或包含非法字符"""
    url_hash = hashlib.md5(url.encode()).hexdigest()[:10]  # 使用 URL 的哈希值
    domain = url.split('//')[-1].split('/')[0][:30]  # 提取域名
    return f"{domain}_{url_hash}"

# 在提取信息部分添加重试机制
def retry_extract(extraction, url, html, max_retries=3, delay=2):
    """重试机制的提取函数"""
    for attempt in range(max_retries):
        try:
            result = extraction.extract(url=url, html=html, ix=0)
            return result
        except Exception as e:
            if attempt < max_retries - 1:  # 如果不是最后一次尝试
                print(f"[WARNING] 第{attempt + 1}次提取失败，{delay}秒后重试: {str(e)}")
                time.sleep(delay)  # 等待一段时间后重试
            else:  # 最后一次尝试也失败
                print(f"[ERROR] 提取失败（已重试{max_retries}次）: {str(e)}")
                return None

async def main():
    # 创建必要的文件夹
    for folder in ["source_codes", "extracted_info"]:
        if not os.path.exists(folder):
            os.makedirs(folder)

    # 示例 URL 列表
    urls = pd.read_excel("url.xlsx")['url'].tolist()

    browser_config = BrowserConfig(
        headless=False,
        verbose=True,
        use_managed_browser=True,
        user_data_dir=r"C:\Users\xm\my_chrome_profile",
        browser_type="chromium",
    )
    dispatcher = SemaphoreDispatcher(
    max_session_permit=10,         # Maximum concurrent tasks
    monitor=CrawlerMonitor(        # Optional monitoring
        max_visible_rows=10,
        display_mode=DisplayMode.DETAILED
    )
)
    # 创建内容过滤器
    prune_filter = PruningContentFilter(
        threshold=0.4,
        threshold_type="fixed",
        min_word_threshold=0
    )
    
    # 创建 LLM 内容选择器
    llM_extraction = LLMExtractionStrategy(
        provider="deepseek/deepseek-chat",  # 使用 deepseek 作为提供商
        api_token="your_deepseek_api",
        instruction="""
        请从网页内容中提取移动套餐相关的信息。请确保返回的是合法的 JSON 格式数据，格式如下：
        {
            "packages": [
                {
                    "公司名称": "运营商名称，如：新加坡电信等",
                    "经营地区": "套餐适用的地区范围",
                    "套餐名称": "具体的套餐名称（必填）",
                    "套餐类别": "套餐的类型，如：5G套餐、4G套餐、预付费套餐等",
                    "数据流量": "套餐包含的流量额度，如500MB/500MB",
                    "价格": "套餐月费或资费标准，如10SGD/月",
                    "计费周期": "月付/季付/年付等",
                    "套餐特点": "套餐的主要特点或优惠内容",
                    "来源url": "来源url"
                }
            ]
        }

        注意事项：
        1. 必须返回合法的 JSON 格式
        2. 套餐名称为必填项，必须提取
        3. 其他字段如果在网页中未找到相关信息，请返回空字符串
        4. 如果找到多个套餐，请在 packages 数组中添加多个对象
        5. 价格请标注具体金额，如"99元/月"
        6. 数据流量请标注具体额度，如"40GB"
        7. 使用页面原本的语言返回即可
        8. 如果没有套餐名称的话，请用数据流量代替
        """,
        chunk_token_threshold=4096,
    )

    # 配置 Markdown 生成器
    md_generator = DefaultMarkdownGenerator(
        content_filter=prune_filter,  # 首先使用 prune_filter
        options={
            "ignore_links": True,
            "ignore_images": True,
            "body_width": 0,
            "escape_html": True
        }
    )
    
    run_cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        excluded_tags=['form', 'header', 'footer', 'nav',"href"],
        exclude_external_links=True,
        remove_overlay_elements=True,
        only_text=True,
        wait_for_images=True,
        markdown_generator=md_generator,
    )

    # 创建一个 DataFrame 来存储所有套餐信息
    all_packages = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun_many(
            urls=urls,
            config=run_cfg,
            dispatcher=dispatcher
        )

        failed_urls = []

        for result in results:
            if result.success:
                safe_name = generate_safe_filename(result.url)
                # 保存过滤后的 HTML 源码和提取的信息
                result_md = result.markdown_v2.fit_html
                if result_md:
                    html_filename = f"source_codes/{safe_name}.html"
                    try:
                        with open(html_filename, "w", encoding="utf-8") as f:
                            f.write(result_md)
                        print(f"[OK] 过滤后的内容保存成功: {html_filename}")

                        # 使用重试机制提取信息
                        llm_result = retry_extract(llM_extraction, result.url, result_md)
                        if llm_result:
                            package_info = {"packages": llm_result}
                            all_packages.extend(package_info['packages'])
                            print(f"[OK] 提取的信息保存成功: 获取到 {len(llm_result)} 个套餐")
                        else:
                            print(f"[WARNING] 未能从 URL 提取到套餐信息: {result.url}")
                            failed_urls.append(result.url)

                    except Exception as e:
                        print(f"[ERROR] 内容处理失败: {str(e)}")

            else:
                print(f"[ERROR] 爬取失败: {result.error_message}")
                failed_urls.append(result.url)

        # 将所有套餐信息保存到 Excel 文件
        if all_packages:
            try:
                df = pd.DataFrame(all_packages)
                df.to_excel("extracted_info/packages_info.xlsx", index=False)
                print(f"[OK] 所有套餐信息已保存到 Excel 文件: extracted_info/packages_info.xlsx")
            except Exception as e:
                print(f"[ERROR] 保存 Excel 文件失败: {str(e)}")

        if failed_urls:
            with open("failed_urls.log", "w", encoding="utf-8") as log_file:
                for url in failed_urls:
                    log_file.write(f"{url}\n")
            print(f"[WARNING] 有 {len(failed_urls)} 个 URL 访问失败，详细信息已记录在 failed_urls.log 中。")

if __name__ == "__main__":
    asyncio.run(main())