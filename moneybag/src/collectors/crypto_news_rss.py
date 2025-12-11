import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime # RSS 날짜 파싱용
import html
import re

class CryptoNewsRSS:
    def __init__(self):
        self.rss_feeds = {
            "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "CoinTelegraph": "https://cointelegraph.com/rss",
            "TheBlock": "https://www.theblock.co/rss.xml",
            "Decrypt": "https://decrypt.co/feed",
            "BitcoinMagazine": "https://bitcoinmagazine.com/.rss/full/",
            "CryptoSlate": "https://cryptoslate.com/feed/",
            "Blockworks": "https://blockworks.co/feed",
            "CoinGape": "https://coingape.com/feed/"
        }
        
        self.keywords = ["ETF", "SEC", "Fed", "Rate", "Binance", "BlackRock", "Regulation", "Hack", "Approval"]

    def fetch_feed(self, source_name, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code != 200: return []

            root = ET.fromstring(response.content)
            items = root.findall('./channel/item')
            if not items: items = root.findall('.//item')

            news_list = []
            for item in items[:5]:
                title = item.find('title').text
                link = item.find('link').text
                desc = item.find('description').text
                
                # [NEW] 날짜 추출 및 변환
                pub_date_str = ""
                pub_element = item.find('pubDate')
                if pub_element is not None and pub_element.text:
                    try:
                        # RSS 표준 날짜 포맷(RFC 822) 파싱 -> YYYY-MM-DD HH:MM 변환
                        dt = parsedate_to_datetime(pub_element.text)
                        pub_date_str = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pub_date_str = pub_element.text[:16] # 파싱 실패 시 앞부분만 사용

                if not title: continue
                
                summary = self._clean_html(desc) if desc else ""
                
                score = 0
                for k in self.keywords:
                    if k.lower() in title.lower(): score += 2
                    if k.lower() in summary.lower(): score += 1
                
                news_list.append({
                    "source": source_name,
                    "title": title.strip(),
                    "link": link,
                    "summary": summary[:250].strip() + "...",
                    "score": score,
                    "published_at": pub_date_str # [NEW] 수집된 날짜 저장
                })
            return news_list
        except:
            return []

    def _clean_html(self, raw_html):
        if not raw_html: return ""
        text = html.unescape(raw_html)
        text = re.sub(r'<[^>]+>', '', text)
        return text.strip()

    def collect_all(self):
        print(f"🌍 글로벌 뉴스 소스 {len(self.rss_feeds)}개 스캔 중...")
        all_news = []
        for name, url in self.rss_feeds.items():
            print(f"   📡 {name}...", end=" ")
            items = self.fetch_feed(name, url)
            print(f"{len(items)}건")
            all_news.extend(items)
        
        all_news.sort(key=lambda x: x['score'], reverse=True)
        
        seen_links = set()
        unique_news = []
        for news in all_news:
            if news['link'] not in seen_links:
                unique_news.append(news)
                seen_links.add(news['link'])
        
        return unique_news[:10]

if __name__ == "__main__":
    collector = CryptoNewsRSS()
    news = collector.collect_all()
    for n in news[:3]:
        print(f"[{n['published_at']}] {n['title']}")