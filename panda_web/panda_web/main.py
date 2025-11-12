import os
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response
import base64
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse

# Import data hub routes
from panda_data_hub.routes.data_clean import factor_data_clean, stock_market_data_clean, financial_data_clean, index_market_data_clean, adj_factor_data_clean
from panda_data_hub.routes.config import config_redefine
from panda_data_hub.routes.query import data_query

app = FastAPI(title="天蝎座量化投资系统 Web Interface")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register data hub API routes
app.include_router(data_query.router, prefix="/datahub/api/v1", tags=["data_query"])
app.include_router(config_redefine.router, prefix="/datahub/api/v1", tags=["config_redefine"])
app.include_router(factor_data_clean.router, prefix="/datahub/api/v1", tags=["factor_data_clean"])
app.include_router(stock_market_data_clean.router, prefix="/datahub/api/v1", tags=["stock_market_data_clean"])
app.include_router(financial_data_clean.router, prefix="/datahub/api/v1", tags=["financial_data_clean"])
app.include_router(index_market_data_clean.router, prefix="/datahub/api/v1", tags=["index_market_data_clean"])
app.include_router(adj_factor_data_clean.router, prefix="/datahub/api/v1", tags=["adj_factor_data_clean"])

# Get the absolute path to the static directory
DIST_DIR = os.path.join(os.path.dirname(__file__), "static")

# Mount the Vue dist directory at /factor path
app.mount("/factor", StaticFiles(directory=DIST_DIR, html=True), name="static")

# Also mount assets at root to handle absolute "/assets/*" requests from bundle
assets_dir = os.path.join(DIST_DIR, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

# Data statistics page
@app.get("/data-statistics")
async def data_statistics():
    """数据列表统计页面"""
    html_file = os.path.join(DIST_DIR, "data_statistics.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# System configuration page
@app.get("/system-config")
async def system_config():
    """系统配置页面"""
    html_file = os.path.join(DIST_DIR, "system_config.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Stock market data cleaning page
@app.get("/stock-market-clean")
async def stock_market_clean():
    """股票行情数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "stock_market_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Factor data cleaning page
@app.get("/factor-data-clean")
async def factor_data_clean_page():
    """因子数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "factor_data_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Financial data cleaning page
@app.get("/financial-data-clean")
async def financial_data_clean_page():
    """财务数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "financial_data_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Index market data cleaning page
@app.get("/index-market-clean")
async def index_market_clean_page():
    """指数行情数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "index_market_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Adj Factor data cleaning page
@app.get("/adj-factor-clean")
async def adj_factor_clean_page():
    """复权因子数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "adj_factor_data_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Valuation Factor data cleaning page
@app.get("/valuation-factor-clean")
async def valuation_factor_clean_page():
    """估值因子数据清洗页面"""
    html_file = os.path.join(DIST_DIR, "valuation_factor_clean.html")
    with open(html_file, 'r', encoding='utf-8') as f:
        return HTMLResponse(content=f.read())

# Create a beautiful navigation homepage
@app.get("/")
async def navigation_home():
    html_content = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>天蝎座量化投资系统 - 导航中心111</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #333;
            }
            
            .container {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 40px;
                box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
                max-width: 800px;
                width: 90%;
            }
            
            .header {
                text-align: center;
                margin-bottom: 40px;
            }
            
            .logo {
                font-size: 2.5rem;
                font-weight: bold;
                background: linear-gradient(45deg, #667eea, #764ba2);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 10px;
            }
            
            .subtitle {
                font-size: 1.1rem;
                color: #666;
                margin-bottom: 20px;
            }
            
            .nav-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .nav-item {
                background: linear-gradient(45deg, #f8f9fa, #e9ecef);
                border: 2px solid transparent;
                border-radius: 15px;
                padding: 25px;
                text-decoration: none;
                transition: all 0.3s ease;
                display: block;
                position: relative;
                overflow: hidden;
                color: #333;
            }
            
            .nav-item::before {
                content: '';
                position: absolute;
                top: 0;
                left: -100%;
                width: 100%;
                height: 100%;
                background: linear-gradient(45deg, #667eea, #764ba2);
                transition: left 0.5s ease;
                z-index: -1;
            }
            
            .nav-item:hover::before {
                left: 0;
            }
            
            .nav-item:hover {
                transform: translateY(-5px);
                box-shadow: 0 15px 30px rgba(0, 0, 0, 0.2);
                color: white;
            }
            
            .nav-title {
                font-size: 1.3rem;
                font-weight: bold;
                margin-bottom: 10px;
                display: flex;
                align-items: center;
            }
            
            .nav-icon {
                font-size: 1.5rem;
                margin-right: 10px;
            }
            
            .nav-desc {
                font-size: 0.95rem;
                opacity: 0.8;
                line-height: 1.4;
            }
            
            .status-bar {
                background: #f8f9fa;
                border-radius: 10px;
                padding: 15px;
                margin-top: 30px;
                border-left: 4px solid #28a745;
            }
            
            .status-title {
                font-weight: bold;
                color: #28a745;
                margin-bottom: 5px;
            }
            
            .status-info {
                font-size: 0.9rem;
                color: #666;
            }
            
            .footer {
                text-align: center;
                margin-top: 20px;
                color: #888;
                font-size: 0.9rem;
            }
            
            .service-status {
                display: inline-block;
                margin: 0 10px;
                padding: 2px 8px;
                border-radius: 10px;
                font-size: 0.85rem;
                font-weight: bold;
            }
            
            .service-running {
                background: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }
            
            .service-stopped {
                background: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }
            
            .service-checking {
                background: #fff3cd;
                color: #856404;
                border: 1px solid #ffeaa7;
            }
            
            .loading {
                animation: pulse 2s infinite;
            }
            
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="logo">♏ 天蝎座</div>
                <div class="subtitle">量化投资系统 · 数据管理中心3333</div>
            </div>
            
            <div class="nav-grid">
                <a href="/factor/" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">🏠</span>
                        因子管理主页
                    </div>
                    <div class="nav-desc">
                        查看因子列表、创建新因子、管理现有因子策略
                    </div>
                </a>
                
                <a href="/system-config" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">⚙️</span>
                        系统配置
                    </div>
                    <div class="nav-desc">
                        配置MongoDB、Tushare Token、数据清洗时间（配置立即生效）
                    </div>
                </a>
                
                <a href="/data-statistics" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📊</span>
                        数据列表查看
                    </div>
                    <div class="nav-desc">
                        查看股票、因子、财务数据统计和数据完整性（支持财务数据）
                    </div>
                </a>
                
                <a href="/stock-market-clean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📈</span>
                        股票行情清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票日线行情数据（开高低收、成交量、成交额等）
                    </div>
                </a>
                
                <a href="/factor-data-clean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📊</span>
                        因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗基础市场因子（市值、换手率、成交额等）
                    </div>
                </a>
                
                <a href="/financial-data-clean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">💰</span>
                        财务数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗财务指标、利润表、资产负债表、现金流量表
                    </div>
                </a>
                
                # <a href="/index-market-clean" class="nav-item">
                #     <div class="nav-title">
                #         <span class="nav-icon">📊</span>
                #         指数行情清洗
                #     </div>
                #     <div class="nav-desc">
                #         清洗主要指数日线行情（上证、深证、沪深300、中证500等）
                #     </div>
                # </a>
                
                <a href="/adj-factor-clean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📈</span>
                        复权因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗股票复权因子数据（adj_factor）
                    </div>
                </a>

                <a href="/valuation-factor-clean" class="nav-item">
                    <div class="nav-title">
                        <span class="nav-icon">📊</span>
                        估值因子数据清洗
                    </div>
                    <div class="nav-desc">
                        清洗估值因子数据（PE-TTM、市销率、市现率、流通市值）
                    </div>
                </a>
            </div>
            
            <div class="status-bar">
                <div class="status-title" style="cursor: pointer;" title="点击刷新状态">系统状态 🔄</div>
                <div class="status-info" id="systemStatus">
                    <span class="loading">正在检测服务状态...</span>
                </div>
            </div>
            
            <div class="footer">
                天蝎座量化投资系统 © 2025 | 天蝎座私募基金公司
            </div>
        </div>
        
        <script>
            // 添加一些交互效果
            document.querySelectorAll('.nav-item').forEach(item => {
                item.addEventListener('click', function(e) {
                    // 添加点击效果
                    this.style.transform = 'scale(0.95)';
                    setTimeout(() => {
                        this.style.transform = 'translateY(-5px)';
                    }, 100);
                });
            });
            
            // 检查服务状态
            async function checkServiceStatus(port, name) {
                try {
                    const response = await fetch(`http://localhost:${port}/`, {
                        method: 'GET',
                        mode: 'cors',
                        timeout: 3000
                    });
                    return {
                        name: name,
                        port: port,
                        status: 'running',
                        message: '运行中'
                    };
                } catch (error) {
                    return {
                        name: name,
                        port: port,
                        status: 'stopped',
                        message: '未启动'
                    };
                }
            }
            
            async function updateSystemStatus() {
                const statusElement = document.getElementById('systemStatus');
                statusElement.innerHTML = '<span class="loading">正在检测服务状态...</span>';
                
                try {
                    // 检查三个服务的状态
                    const services = [
                        { port: 8080, name: '前端服务' },
                        { port: 8111, name: '后端API' },
                        { port: 8222, name: '数据服务' }
                    ];
                    
                    const statusPromises = services.map(service => 
                        checkServiceStatus(service.port, service.name)
                    );
                    
                    const results = await Promise.all(statusPromises);
                    
                    // 构建状态显示
                    let statusHTML = '';
                    results.forEach(result => {
                        const statusClass = result.status === 'running' ? 'service-running' : 'service-stopped';
                        const statusIcon = result.status === 'running' ? '✅' : '❌';
                        statusHTML += `<span class="service-status ${statusClass}">${statusIcon} ${result.name}：${result.message} (${result.port}端口)</span>`;
                    });
                    
                    statusElement.innerHTML = statusHTML;
                    
                } catch (error) {
                    statusElement.innerHTML = '<span class="service-checking">服务状态检测失败</span>';
                }
            }
            
            // 页面加载完成后检查状态
            document.addEventListener('DOMContentLoaded', function() {
                console.log('Scorpio Quant System Navigation Center Loaded');
                updateSystemStatus();
                
                // 每30秒自动检查一次服务状态
                setInterval(updateSystemStatus, 30000);
            });
            
            // 添加手动刷新状态的功能
            document.querySelector('.status-title').addEventListener('click', function() {
                updateSystemStatus();
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# Fallback 1x1 transparent PNG for missing chat asset to avoid 404 noise
_TRANSPARENT_PNG_BASE64 = (
    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Qm1cEwAAAAASUVORK5CYII="
)

@app.get("/assets/chat-dI4p2fsV.png")
async def _fallback_chat_png_main():
    try:
        file_path = os.path.join(assets_dir, "chat-dI4p2fsV.png")
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                return Response(content=f.read(), media_type="image/png")
    except Exception:
        pass
    return Response(content=base64.b64decode(_TRANSPARENT_PNG_BASE64), media_type="image/png")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True  # Enable auto-reload during development
    ) 