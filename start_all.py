#!/usr/bin/env python
"""
天蝎座量化投资系统 统一启动脚本
一键启动所有服务
"""
import os
import sys
import time
import subprocess
import signal
from pathlib import Path

# 颜色输出
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")

def print_success(text):
    print(f"{Colors.OKGREEN}✓ {text}{Colors.ENDC}")

def print_info(text):
    print(f"{Colors.OKBLUE}ℹ {text}{Colors.ENDC}")

def print_warning(text):
    print(f"{Colors.WARNING}⚠ {text}{Colors.ENDC}")

def print_error(text):
    print(f"{Colors.FAIL}✗ {text}{Colors.ENDC}")

# 存储所有子进程
processes = []

def cleanup():
    """清理所有子进程"""
    print_info("正在关闭所有服务...")
    for name, process in processes:
        if process.poll() is None:  # 进程还在运行
            print_info(f"停止 {name}...")
            process.terminate()
            try:
                process.wait(timeout=5)
                print_success(f"{name} 已停止")
            except subprocess.TimeoutExpired:
                print_warning(f"{name} 超时，强制终止...")
                process.kill()
    print_success("所有服务已关闭")

def signal_handler(sig, frame):
    """处理Ctrl+C信号"""
    print_warning("\n\n接收到中断信号，正在优雅关闭...")
    cleanup()
    sys.exit(0)

# 注册信号处理
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def check_port(port):
    """检查端口是否被占用"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def start_service(name, command, cwd=None, wait_time=2, check_port_num=None):
    """启动一个服务"""
    print_info(f"启动 {name}...")
    
    # 检查端口
    if check_port_num and check_port(check_port_num):
        print_warning(f"端口 {check_port_num} 已被占用，跳过 {name}")
        return None
    
    try:
        # Windows使用不同的命令
        if sys.platform == 'win32':
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
                preexec_fn=os.setsid
            )
        
        # 等待服务启动
        time.sleep(wait_time)
        
        # 检查进程是否还在运行
        if process.poll() is None:
            print_success(f"{name} 已启动 (PID: {process.pid})")
            processes.append((name, process))
            return process
        else:
            print_error(f"{name} 启动失败")
            return None
    except Exception as e:
        print_error(f"{name} 启动失败: {str(e)}")
        return None

def main():
    print_header("天蝎座量化投资系统")
    print_info("统一启动所有服务...")
    
    # 获取项目根目录
    root_dir = Path(__file__).parent.resolve()
    print_info(f"项目根目录: {root_dir}")
    
    # 服务配置
    services = [
        {
            "name": "Web服务 (前端 + 数据API)",
            "command": "python web_server.py",
            "cwd": str(root_dir),
            "port": 19080,
            "wait_time": 3,
            "url": "http://localhost:19080"
        },
        {
            "name": "数据自动更新任务",
            "command": "python -m panda_data_hub._main_auto_",
            "cwd": str(root_dir),
            "port": None,
            "wait_time": 2,
            "url": None
        }
    ]
    
    print_header("启动服务")
    
    # 启动所有服务
    started_services = []
    for service in services:
        result = start_service(
            service["name"],
            service["command"],
            service["cwd"],
            service["wait_time"],
            service["port"]
        )
        if result:
            started_services.append(service)
    
    if not started_services:
        print_error("没有服务成功启动")
        return
    
    # 显示服务状态
    print_header("服务状态")
    print_info(f"共启动 {len(started_services)} 个服务\n")
    
    for service in started_services:
        print(f"{Colors.OKGREEN}● {service['name']}{Colors.ENDC}")
        if service['port']:
            print(f"  端口: {service['port']}")
        if service['url']:
            print(f"  访问: {Colors.OKCYAN}{service['url']}{Colors.ENDC}")
        print()
    
    print_header("服务访问地址")
    print(f"{Colors.BOLD}前端界面:{Colors.ENDC}")
    print(f"  {Colors.OKCYAN}http://localhost:19080/{Colors.ENDC}")
    print(f"  {Colors.OKCYAN}http://localhost:19080/factor/{Colors.ENDC} (天蝎座量化投资系统)")
    print()
    print(f"{Colors.BOLD}API文档:{Colors.ENDC}")
    print(f"  {Colors.OKCYAN}http://localhost:19080/docs{Colors.ENDC} (数据清洗API)")
    print(f"  {Colors.OKCYAN}http://localhost:19111/docs{Colors.ENDC} (因子计算API)")
    print()
    
    print_header("控制说明")
    print(f"  {Colors.WARNING}按 Ctrl+C 停止所有服务{Colors.ENDC}")
    print(f"  {Colors.OKBLUE}服务日志将在下方实时显示{Colors.ENDC}")
    print()
    
    # 保持运行
    try:
        while True:
            time.sleep(1)
            # 检查服务是否还在运行
            for name, process in processes[:]:
                if process.poll() is not None:
                    print_error(f"{name} 意外退出 (退出码: {process.poll()})")
                    processes.remove((name, process))
            
            if not processes:
                print_error("所有服务都已停止")
                break
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_error(f"启动失败: {str(e)}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)

