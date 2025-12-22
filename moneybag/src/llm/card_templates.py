# Base CSS for all cards
BASE_CSS = """
<style>
    body {
        font-family: 'Pretendard', sans-serif;
        background-color: #191c24;
        color: #ebeeef;
        margin: 0;
        padding: 60px;
        width: 1080px;
        height: 1080px;
        box-sizing: border-box;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    h1 { font-size: 72px; color: #ffffff; margin: 0 0 20px 0; line-height: 1.2; }
    h2 { font-size: 48px; color: #b46eff; margin: 0 0 40px 0; }
    p { font-size: 32px; line-height: 1.8; color: #c5c8d3; margin: 0; }
    .content { width: 100%; }
</style>
"""

def get_commander_briefing_template(title, subtitle, quote):
    """02_commander_briefing 카드 템플릿"""
    css = f"""
    {BASE_CSS}
    <style>
        /* [개선] 좌측 정렬 및 상단 여백 추가로 허전함 해소 */
        body {{
            align-items: flex-start;
            text-align: left;
            padding-top: 120px; 
        }}
        h1 {{ font-size: 64px; }}
        h2 {{ font-size: 40px; margin-bottom: 60px; }}
        p {{
            font-size: 48px;
            font-style: italic;
            line-height: 1.6;
            border-left: 5px solid #b46eff;
            padding-left: 40px;
        }}
    </style>
    """
    html = f"""
    <html><head>{css}</head><body>
        <div class="content">
            <h2>{subtitle}</h2>
            <h1>{title}</h1>
            <p>"{quote}"</p>
        </div>
    </body></html>
    """
    return html

def get_news_template(title, subtitle, text):
    """07_news 카드 템플릿"""
    css = f"""
    {BASE_CSS}
    <style>
        /* [개선] 텍스트 잘림 방지를 위해 전체적인 패딩 증가 */
        body {{ padding: 80px; }}
        p {{ word-break: keep-all; overflow-wrap: break-word; }}
    </style>
    """
    html = f"""
    <html><head>{css}</head><body>
        <div class="content">
            <h2>{subtitle}</h2>
            <h1>{title}</h1>
            <p>{text}</p>
        </div>
    </body></html>
    """
    return html