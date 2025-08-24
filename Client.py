#!/usr/bin/env python3
"""
Socket Client for testing data transfer from server.
Features:
    -s : server IP address
    -p : server port number
    -v : data size to receive (in KB)
    -h : display help
"""

import socket
import sys
import argparse
import time
import logging
import os


def get_client_ip():
    """Get the client's local IP address."""
    try:
        # 创建一个临时socket来获取本机IP
        temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        temp_socket.connect(("8.8.8.8", 80))  # 连接到公共DNS服务器
        client_ip = temp_socket.getsockname()[0]
        temp_socket.close()
        return client_ip
    except Exception:
        try:
            # 备用方法：获取主机名对应的IP
            hostname = socket.gethostname()
            client_ip = socket.gethostbyname(hostname)
            return client_ip
        except Exception:
            return "localhost"


def setup_logging():
    """Setup logging configuration to file named as client_ip:port.txt."""
    # 获取客户端IP和随机端口
    client_ip = get_client_ip()

    # 创建一个临时socket来获取可用端口
    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    temp_socket.bind(('', 0))  # 绑定到随机可用端口
    client_port = temp_socket.getsockname()[1]
    temp_socket.close()

    # 创建日志文件名（客户端IP:端口.txt）
    log_filename = f"log/{client_ip}:{client_port}.txt"

    # 如果文件已存在，先删除它
    if os.path.exists(log_filename):
        os.remove(log_filename)

    # 设置日志配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=log_filename,
        filemode='w'
    )

    # 同时输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

    logging.info(f"Client IP: {client_ip}, Client Port: {client_port}")
    logging.info(f"Logging to file: {log_filename}")

    return client_ip, client_port


def receive_data(server_ip, server_port, data_size_kb, client_ip, client_port):
    """
    Connect to server and receive specified amount of data.

    Args:
        server_ip (str): Server IP address
        server_port (int): Server port number
        data_size_kb (int): Data size to receive in KB
        client_ip (str): Client IP address
        client_port (int): Client port number

    Returns:
        bool: True if successful, False otherwise
    """
    # Convert KB to bytes
    data_size_bytes = data_size_kb * 1024

    try:
        # Create TCP socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # 绑定到特定的客户端端口
        client_socket.bind((client_ip, client_port))

        # Set timeout to 10 seconds
        client_socket.settimeout(10)

        logging.info(f"Connecting from {client_ip}:{client_port} to server {server_ip}:{server_port}...")

        # Connect to server
        client_socket.connect((server_ip, server_port))
        logging.info(f"Connected to server from {client_ip}:{client_port}")

        # Send request for data
        request = f"GET_DATA {data_size_kb}KB\n"
        client_socket.send(request.encode())
        logging.info(f"Requested {data_size_kb}KB of data")

        # Receive data
        total_received = 0
        start_time = time.time()

        while total_received < data_size_bytes:
            # Calculate remaining bytes to receive
            remaining = data_size_bytes - total_received
            # Receive up to 4096 bytes at a time
            chunk_size = min(4096, remaining)

            try:
                data = client_socket.recv(chunk_size)
                if not data:
                    logging.error("Connection closed by server")
                    return False

                received = len(data)
                total_received += received

                # Log progress every 64KB
                if total_received % (64 * 1024) == 0 or total_received == data_size_bytes:
                    progress = (total_received / data_size_bytes) * 100
                    logging.info(f"Received {total_received / 1024:.2f}KB ({progress:.1f}%)")

            except socket.timeout:
                logging.error("Receive operation timed out")
                return False
            except Exception as e:
                logging.error(f"Error receiving data: {e}")
                return False

        end_time = time.time()
        duration = end_time - start_time

        # Calculate transfer statistics
        size_mb = total_received / (1024 * 1024)
        speed_mbps = (total_received * 8) / (duration * 1000000)  # Mbps

        logging.info(f"Transfer completed: {total_received} bytes received")
        logging.info(f"Time taken: {duration:.2f} seconds")
        logging.info(f"Average speed: {speed_mbps:.2f} Mbps")

        return True

    except socket.timeout:
        logging.error("Connection timed out")
        return False
    except ConnectionRefusedError:
        logging.error(f"Connection refused by server {server_ip}:{server_port}")
        return False
    except Exception as e:
        logging.error(f"Error: {e}")
        return False
    finally:
        try:
            client_socket.close()
            logging.info("Connection closed")
        except:
            pass


def main():
    """Parse command line arguments and run client."""
    parser = argparse.ArgumentParser(
        description="Socket Client for data transfer testing",
        add_help=False
    )

    parser.add_argument(
        "-s", "--server",
        type=str,
        required=True,
        help="server IP address"
    )

    parser.add_argument(
        "-p", "--port",
        type=int,
        default=5001,
        help="server port number (default: 5001)"
    )

    parser.add_argument(
        "-v", "--size",
        type=int,
        required=True,
        help="data size to receive (in KB)"
    )

    parser.add_argument(
        "-h", "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="display this help message and exit"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.size <= 0:
        print("Error: Data size must be positive", file=sys.stderr)
        sys.exit(1)

    if not (0 < args.port <= 65535):
        print("Error: Port must be between 1 and 65535", file=sys.stderr)
        sys.exit(1)

    # 设置日志，使用客户端IP和端口作为文件名
    client_ip, client_port = setup_logging()

    logging.info(
        f"Starting client: client={client_ip}:{client_port}, server={args.server}:{args.port}, size={args.size}KB")

    # Connect to server and receive data
    success = receive_data(args.server, args.port, args.size, client_ip, client_port)

    if success:
        logging.info("Data transfer completed successfully")
        sys.exit(0)
    else:
        logging.error("Data transfer failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

