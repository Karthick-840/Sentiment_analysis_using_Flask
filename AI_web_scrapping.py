import os
import sys
import psutil
import subprocess
import requests
import asyncio
import logging
from xml.etree import ElementTree
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator


class Crawl4aiWebScraper:
    def __init__(self, url,filename = None, get_all_page=False,logger=None):
        self.url = url
        self.filename = filename
        self.get_all_page = get_all_page
        self.logger = logger if logger else logging.getLogger(__name__)
        self.crawler = AsyncWebCrawler()
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def web_to_markdown(self):
        if not self.get_all_page: # gets only start page.
            try:
                content = asyncio.run(self.get_start_page())
            except Exception as e:
                    self.logger.info(f"Start Page crawling failed: {e}.")
                    content = []
        else:
            try:
                content = asyncio.run(self.crawl_parallel())
            except Exception as e:
                self.logger.info(f"Parallel crawling failed: {e}. Trying sequential crawling...")
                try:
                    content = asyncio.run(self.crawl_sequential())
                except Exception as e:
                    self.logger.info(f"Sequential crawling also failed: {e}.")
                    content = []

        Crawl4aiWebScraper.save_to_file(content, filename=self.filename if self.filename else "output.md")

    async def get_start_page(self):
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun( url=self.url,)
            return  result.markdown
            
    async def crawl_sequential(self):
        urls = Crawl4aiWebScraper.get_site_content_urls(self.url)
        self.logger.info("\n=== Sequential Crawling with Session Reuse ===")

        browser_config = BrowserConfig(
            headless=True,
            # For better performance in Docker or low-memory environments:
            extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
        )

        crawl_config = CrawlerRunConfig(
            markdown_generator=DefaultMarkdownGenerator()
        )

        # Create the crawler (opens the browser)
        crawler = AsyncWebCrawler(config=browser_config)
        await crawler.start()

        all_markdown_content = []  # List to store all crawled content

        try:
            session_id = "session1"  # Reuse the same session across all URLs
            for url in urls:
                result = await crawler.arun(
                    url=url,
                    config=crawl_config,
                    session_id=session_id
                )
                if result.success:
                    self.logger.info(f"Successfully crawled: {url}")
                    # E.g. check markdown length
                    self.logger.info(f"Markdown length: {len(result.markdown)}")
                    # Save the markdown content along with the URL as a dictionary
                    all_markdown_content.append({
                        "url": url,
                        "content": result.markdown
                    })
                else:
                    self.logger.info(f"Failed: {url} - Error: {result.error_message}")
        finally:
            # After all URLs are done, close the crawler (and the browser)
            await crawler.close()

        # Save all content to a file
        return all_markdown_content

    async def crawl_parallel(self, max_concurrent: int = 3):
        self.logger.info("\n=== Parallel Crawling with Browser Reuse + Memory Check ===")
        urls = Crawl4aiWebScraper.get_site_content_urls(self.url)

        # Minimal browser config
        browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
        )
        crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

        # Create the crawler instance
        crawler = AsyncWebCrawler(config=browser_config)
        await crawler.start()

        all_markdown_content = []  # To store combined markdown content

        try:
            # We'll chunk the URLs in batches of 'max_concurrent'
            success_count = 0
            fail_count = 0
            for i in range(0, len(urls), max_concurrent):
                batch = urls[i : i + max_concurrent]
                tasks = []

                for j, url in enumerate(batch):
                    # Unique session_id per concurrent sub-task
                    session_id = f"parallel_session_{i + j}"
                    task = crawler.arun(url=url, config=crawl_config, session_id=session_id)
                    tasks.append(task)

                # Gather results
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Evaluate results
                for url, result in zip(batch, results):
                    if isinstance(result, Exception):
                        self.logger.info(f"Error crawling {url}: {result}")
                        fail_count += 1
                    elif result.success:
                        self.logger.info(f"Successfully crawled: {url}")
                        success_count += 1
                        
                    # E.g. check markdown length
                        self.logger.info(f"Markdown length: {len(result.markdown)}")
                    # Save the markdown content along with the URL as a dictionary
                        all_markdown_content.append({"url": url,"content": result.markdown})
                    else:
                        fail_count += 1

            self.logger.info(f"\nSummary:")
            self.logger.info(f"  - Successfully crawled: {success_count}")
            self.logger.info(f"  - Failed: {fail_count}")

        finally:
            self.logger.info("\nClosing crawler...")
            await crawler.close()

        return all_markdown_content  # Return combined markdown content

    @staticmethod
    def get_site_content_urls(url):
        sitemap_url = url.rstrip('/') + '/sitemap.xml'
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status()
            
            # Parse the XML
            root = ElementTree.fromstring(response.content)
            print(response.content)
   
            
            # Extract all URLs from the sitemap
            # The namespace is usually defined in the root element
            #namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            namespace = {'ns': root.tag.split('}')[0].strip('{')}
            urls = [loc.text for loc in root.findall('.//ns:loc', namespaces=namespace)]
            if not urls:
                print("No URLs found.")
            return urls
        
        except Exception as e:
            print(f"Error fetching sitemap: {e}")
            return []        

    @staticmethod
    def verify_dependencies():
        try:
            result = subprocess.run(["crawl4ai-doctor"], capture_output=True, text=True, check=True)
            if "[COMPLETE] ● ✅ Crawling test passed!" in result.stdout:
                print("[COMPLETE] ● ✅ Crawling test passed!")
                return (True,("[COMPLETE] ● ✅ Crawling test passed!"))
            else:
                return result.stdout
        except subprocess.CalledProcessError as e:
            print("Error during verification. Output:")
            print(e.output)
            print("Attempting to install dependencies...")
            try:
                subprocess.run(["pip", "install", "--U", "crawl4ai"], check=True)
                subprocess.run(["crawl4ai-setup"], check=True)
            except subprocess.CalledProcessError:
                subprocess.run(["python", "-m", "playwright", "install", "--with-deps", "chromium"], check=True)
            print("Dependencies installed. Please re-run the verification.")

    @staticmethod
    def save_to_file(content, filename="output.md"):
        """
        Saves content to a file. Supports both string and list of dictionaries.
        Args:
            content (str or list): The content to save. Can be a string or a list of dictionaries.
            filename (str): The name of the file to save the content to.
        """
        docs_dir = "docs"
        if not os.path.exists(docs_dir):
            os.makedirs(docs_dir) 
        filename = os.path.join(docs_dir, filename)

        with open(filename, "w", encoding="utf-8") as file:
            if isinstance(content, str):
                # If content is a string, write it directly
                file.write(content)
            elif isinstance(content, list) and all(isinstance(item, dict) for item in content):
                # If content is a list of dictionaries, format it as Markdown
                for item in content:
                    url = item.get("url", "No URL")
                    markdown_content = item.get("content", "No Content")
                    file.write(f"## {url}\n\n")
                    file.write(f"{markdown_content}\n\n")
                    file.write("---\n\n")
            else:
                raise ValueError("Content must be a string or a list of dictionaries.")
        print(f"file is written to {filename}")


