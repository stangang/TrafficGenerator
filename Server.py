#!/usr/bin/env python3
"""
Socket Server with command line arguments support and data transfer capability.
Features:
    -p : TCP port (default 5001)
    -v : verbose output
    -d : run as daemon
    -h : display help
"""

import socket
import sys
import argparse
import os
import time
import signal
import logging
from datetime import datetime


def daemonize():
    """Daemonize the current process."""
    try:
        pid = os.fork()
        if pid > 0:
            # Exit first parent
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"Fork #1 failed: {e}\n")
        sys.exit(1)

    # Decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # Do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # Exit from second parent
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"Fork #2 failed: {e}\n")
        sys.exit(1)

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(os.devnull, 'r')
    so = open(os.devnull, 'a+')
    se = open(os.devnull, 'a+')

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def setup_logging(verbose=False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def handle_data_request(client_socket, size_kb, address, verbose):
    """
    Handle data request from client by sending the specified amount of data.

    Args:
        client_socket: Client socket object
        size_kb: Data size to send in KB
        address: Client address
        verbose: Verbose logging flag
    """
    size_bytes = size_kb * 1024

    if verbose:
        logging.info(f"Preparing to send {size_kb}KB ({size_bytes} bytes) to {address}")

    try:
        # Send acknowledgment
        ack_msg = f"DATA_START {size_kb}KB\n"
        client_socket.send(ack_msg.encode())

        # Generate and send data in chunks
        chunk_size = 4096  # 4KB chunks
        sent = 0

        # Pre-generate a chunk of data to avoid creating it in the loop
        data_chunk = b'X' * chunk_size  # Using 'X' characters instead of null bytes

        start_time = time.time()

        while sent < size_bytes:
            remaining = size_bytes - sent
            current_chunk_size = min(chunk_size, remaining)

            # For the last chunk, we might need a smaller size
            if current_chunk_size < chunk_size:
                chunk_data = b'X' * current_chunk_size
            else:
                chunk_data = data_chunk

            try:
                bytes_sent = client_socket.send(chunk_data)
                if bytes_sent == 0:
                    logging.error(f"Connection broken while sending to {address}")
                    return False

                sent += bytes_sent

                # Log progress every 1MB or when complete
                if verbose and (sent % (1024 * 1024) == 0 or sent == size_bytes):
                    progress = (sent / size_bytes) * 100
                    logging.debug(f"Sent {sent / 1024:.2f}KB to {address} ({progress:.1f}%)")

            except BrokenPipeError:
                logging.error(f"Client {address} disconnected during transfer")
                return False
            except Exception as e:
                logging.error(f"Error sending data to {address}: {e}")
                return False

        end_time = time.time()
        duration = end_time - start_time

        # Calculate transfer statistics
        speed_mbps = (size_bytes * 8) / (duration * 1000000)  # Mbps

        if verbose:
            logging.info(f"Sent {size_kb}KB to {address} in {duration:.2f} seconds")
            logging.info(f"Transfer speed: {speed_mbps:.2f} Mbps")

        # Send completion message
        complete_msg = f"DATA_COMPLETE {size_kb}KB\n"
        client_socket.send(complete_msg.encode())

        return True

    except Exception as e:
        logging.error(f"Error handling data request for {address}: {e}")
        return False


def handle_client(client_socket, address, verbose):
    """Handle incoming client connection."""
    if verbose:
        logging.info(f"Connection from {address}")

    try:
        # Send welcome message
        welcome_msg = f"Welcome to the Data Server! Current time: {datetime.now()}\r\n"
        welcome_msg += "Available commands:\r\n"
        welcome_msg += "  GET_DATA <size>KB - Request data of specified size\r\n"
        welcome_msg += "  ECHO <message> - Echo back the message\r\n"
        welcome_msg += "  QUIT - Disconnect from server\r\n"
        client_socket.send(welcome_msg.encode())

        while True:
            try:
                data = client_socket.recv(1024)
                if not data:
                    if verbose:
                        logging.info(f"Client {address} disconnected")
                    break

                message = data.decode().strip()
                if verbose:
                    logging.debug(f"Received from {address}: {message}")

                # Handle QUIT command
                if message.upper() == "QUIT":
                    if verbose:
                        logging.info(f"Client {address} requested disconnect")
                    client_socket.send(b"Goodbye!\n")
                    break

                # Handle data request
                elif message.upper().startswith("GET_DATA"):
                    try:
                        # Parse requested size
                        parts = message.split()
                        if len(parts) >= 2:
                            size_str = parts[1].upper()
                            if size_str.endswith("KB"):
                                size_kb = int(size_str[:-2])

                                if size_kb <= 0:
                                    error_msg = "ERROR: Size must be positive\n"
                                    client_socket.send(error_msg.encode())
                                    continue

                                # Limit maximum size to prevent abuse (1GB max)
                                max_size_kb = 1024 * 1024  # 1GB
                                if size_kb > max_size_kb:
                                    error_msg = f"ERROR: Maximum size is {max_size_kb}KB\n"
                                    client_socket.send(error_msg.encode())
                                    continue

                                # Handle the data request
                                success = handle_data_request(client_socket, size_kb, address, verbose)
                                if not success:
                                    break
                                continue
                    except ValueError:
                        pass

                    # If we get here, the request was invalid
                    error_msg = "ERROR: Invalid data request format. Use: GET_DATA <size>KB\n"
                    client_socket.send(error_msg.encode())

                # Handle ECHO command
                elif message.upper().startswith("ECHO"):
                    if len(message) > 4:
                        echo_text = message[4:].strip()
                        response = f"ECHO: {echo_text}\n"
                    else:
                        response = "ECHO: (no message)\n"
                    client_socket.send(response.encode())

                # Default response for unknown commands
                else:
                    response = "Unknown command. Available: GET_DATA, ECHO, QUIT\n"
                    client_socket.send(response.encode())

            except socket.timeout:
                logging.warning(f"Client {address} timeout")
                break
            except ConnectionResetError:
                logging.warning(f"Client {address} connection reset")
                break
            except Exception as e:
                logging.error(f"Error processing client {address} message: {e}")
                break

    except Exception as e:
        logging.error(f"Error handling client {address}: {e}")
    finally:
        client_socket.close()
        if verbose:
            logging.info(f"Connection with {address} closed")


def run_server(port, verbose, daemon):
    """Run the socket server with given parameters."""
    if daemon:
        daemonize()

    setup_logging(verbose)

    # Create a TCP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Set socket timeout to handle client connections properly
    server_socket.settimeout(1)

    # Bind the socket to the port
    server_address = ('', port)
    try:
        server_socket.bind(server_address)
        logging.info(f"Server started on port {port}")
    except Exception as e:
        logging.error(f"Failed to bind to port {port}: {e}")
        sys.exit(1)

    # Listen for incoming connections
    server_socket.listen(5)
    logging.info("Waiting for connections...")

    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logging.info("Shutting down server...")
        server_socket.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Main server loop
    try:
        while True:
            try:
                client_socket, address = server_socket.accept()
                # Set timeout for client socket
                client_socket.settimeout(30)  # 30 second timeout
                handle_client(client_socket, address, verbose)
            except socket.timeout:
                # This is expected due to server socket timeout
                continue
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
    except KeyboardInterrupt:
        logging.info("Server interrupted by user")
    finally:
        server_socket.close()
        logging.info("Server shutdown complete")


def main():
    """Parse command line arguments and start server."""
    parser = argparse.ArgumentParser(
        description="Socket Server with command line arguments support",
        add_help=False
    )

    parser.add_argument(
        "-p", "--port",
        type=int,
        default=5001,
        help="TCP port that the server listens on (default: 5001)"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="give more detailed output"
    )

    parser.add_argument(
        "-d", "--daemon",
        action="store_true",
        help="run the server as a daemon"
    )

    parser.add_argument(
        "-h", "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="display this help message and exit"
    )

    args = parser.parse_args()

    # Validate port number
    if not (0 < args.port <= 65535):
        print("Error: Port must be between 1 and 65535", file=sys.stderr)
        sys.exit(1)

    run_server(args.port, args.verbose, args.daemon)


if __name__ == "__main__":
    main()


# !/usr/bin/env python3
"""
Socket Server with command line arguments support and data transfer capability.
Supports multiple concurrent client connections.
"""

# import socket
# import sys
# import argparse
# import os
# import time
# import signal
# import logging
# import threading
# from datetime import datetime
# from collections import defaultdict
#
#
# class SocketServer:
#     def __init__(self, port=5001, verbose=False, daemon=False):
#         self.port = port
#         self.verbose = verbose
#         self.daemon = daemon
#         self.active_connections = defaultdict(int)
#         self.connection_lock = threading.Lock()
#         self.running = False
#
#     def setup_logging(self):
#         """Setup logging configuration."""
#         level = logging.DEBUG if self.verbose else logging.INFO
#         logging.basicConfig(
#             level=level,
#             format='%(asctime)s - %(levelname)s - %(message)s',
#             datefmt='%Y-%m-%d %H:%M:%S'
#         )
#
#     def handle_client(self, client_socket, address):
#         """Handle incoming client connection."""
#         client_ip, client_port = address
#
#         with self.connection_lock:
#             self.active_connections[client_ip] += 1
#             active_count = self.active_connections[client_ip]
#
#         if self.verbose:
#             logging.info(f"Connection from {address} (active: {active_count})")
#
#         try:
#             # Send welcome message
#             welcome_msg = f"Welcome to the Data Server! Current time: {datetime.now()}\r\n"
#             welcome_msg += "Available commands:\r\n"
#             welcome_msg += "  GET_DATA <size>KB - Request data of specified size\r\n"
#             welcome_msg += "  ECHO <message> - Echo back the message\r\n"
#             welcome_msg += "  QUIT - Disconnect from server\r\n"
#             client_socket.send(welcome_msg.encode())
#
#             while self.running:
#                 try:
#                     data = client_socket.recv(1024)
#                     if not data:
#                         if self.verbose:
#                             logging.info(f"Client {address} disconnected gracefully")
#                         break
#
#                     message = data.decode().strip()
#                     if self.verbose:
#                         logging.debug(f"Received from {address}: {message}")
#
#                     # Handle QUIT command
#                     if message.upper() == "QUIT":
#                         if self.verbose:
#                             logging.info(f"Client {address} requested disconnect")
#                         client_socket.send(b"Goodbye!\n")
#                         break
#
#                     # Handle data request
#                     elif message.upper().startswith("GET_DATA"):
#                         response = self.handle_data_request(message, client_socket, address)
#                         if response == "DISCONNECT":
#                             break
#
#                     # Handle ECHO command
#                     elif message.upper().startswith("ECHO"):
#                         if len(message) > 4:
#                             echo_text = message[4:].strip()
#                             response = f"ECHO: {echo_text}\n"
#                         else:
#                             response = "ECHO: (no message)\n"
#                         client_socket.send(response.encode())
#
#                     # Default response for unknown commands
#                     else:
#                         response = "Unknown command. Available: GET_DATA, ECHO, QUIT\n"
#                         client_socket.send(response.encode())
#
#                 except socket.timeout:
#                     if self.verbose:
#                         logging.debug(f"Client {address} timeout, keeping connection alive")
#                     continue
#                 except ConnectionResetError:
#                     if self.verbose:
#                         logging.warning(f"Client {address} connection reset")
#                     break
#                 except Exception as e:
#                     logging.error(f"Error processing client {address} message: {e}")
#                     break
#
#         except Exception as e:
#             logging.error(f"Error handling client {address}: {e}")
#         finally:
#             client_socket.close()
#             with self.connection_lock:
#                 self.active_connections[client_ip] = max(0, self.active_connections[client_ip] - 1)
#             if self.verbose:
#                 logging.info(f"Connection with {address} closed")
#
#     def handle_data_request(self, message, client_socket, address):
#         """Handle data request from client."""
#         try:
#             # Parse requested size
#             parts = message.split()
#             if len(parts) >= 2:
#                 size_str = parts[1].upper()
#                 if size_str.endswith("KB"):
#                     size_kb = int(size_str[:-2])
#
#                     if size_kb <= 0:
#                         error_msg = "ERROR: Size must be positive\n"
#                         client_socket.send(error_msg.encode())
#                         return "CONTINUE"
#
#                     # Limit maximum size to prevent abuse (1GB max)
#                     max_size_kb = 1024 * 1024  # 1GB
#                     if size_kb > max_size_kb:
#                         error_msg = f"ERROR: Maximum size is {max_size_kb}KB\n"
#                         client_socket.send(error_msg.encode())
#                         return "CONTINUE"
#
#                     # Handle the data request
#                     return self.send_data_to_client(client_socket, size_kb, address)
#         except ValueError:
#             pass
#
#         # If we get here, the request was invalid
#         error_msg = "ERROR: Invalid data request format. Use: GET_DATA <size>KB\n"
#         client_socket.send(error_msg.encode())
#         return "CONTINUE"
#
#     def send_data_to_client(self, client_socket, size_kb, address):
#         """Send data to client."""
#         size_bytes = size_kb * 1024
#
#         if self.verbose:
#             logging.info(f"Preparing to send {size_kb}KB ({size_bytes} bytes) to {address}")
#
#         try:
#             # Send acknowledgment
#             ack_msg = f"DATA_START {size_kb}KB\n"
#             client_socket.send(ack_msg.encode())
#
#             # Generate and send data in chunks
#             chunk_size = 4096  # 4KB chunks
#             sent = 0
#
#             # Pre-generate a chunk of data
#             data_chunk = b'X' * chunk_size
#
#             start_time = time.time()
#
#             while sent < size_bytes and self.running:
#                 remaining = size_bytes - sent
#                 current_chunk_size = min(chunk_size, remaining)
#
#                 # For the last chunk, we might need a smaller size
#                 if current_chunk_size < chunk_size:
#                     chunk_data = b'X' * current_chunk_size
#                 else:
#                     chunk_data = data_chunk
#
#                 try:
#                     bytes_sent = client_socket.send(chunk_data)
#                     if bytes_sent == 0:
#                         logging.error(f"Connection broken while sending to {address}")
#                         return "DISCONNECT"
#
#                     sent += bytes_sent
#
#                     # Log progress every 1MB or when complete
#                     if self.verbose and (sent % (1024 * 1024) == 0 or sent == size_bytes):
#                         progress = (sent / size_bytes) * 100
#                         logging.debug(f"Sent {sent / 1024:.2f}KB to {address} ({progress:.1f}%)")
#
#                 except BrokenPipeError:
#                     logging.error(f"Client {address} disconnected during transfer")
#                     return "DISCONNECT"
#                 except Exception as e:
#                     logging.error(f"Error sending data to {address}: {e}")
#                     return "DISCONNECT"
#
#             end_time = time.time()
#             duration = end_time - start_time
#
#             # Calculate transfer statistics
#             speed_mbps = (size_bytes * 8) / (duration * 1000000) if duration > 0 else 0
#
#             if self.verbose:
#                 logging.info(f"Sent {size_kb}KB to {address} in {duration:.2f} seconds")
#                 logging.info(f"Transfer speed: {speed_mbps:.2f} Mbps")
#
#             # Send completion message
#             complete_msg = f"DATA_COMPLETE {size_kb}KB\n"
#             client_socket.send(complete_msg.encode())
#
#             return "CONTINUE"
#
#         except Exception as e:
#             logging.error(f"Error handling data request for {address}: {e}")
#             return "DISCONNECT"
#
#     def run(self):
#         """Run the socket server."""
#         if self.daemon:
#             self.daemonize()
#
#         self.setup_logging()
#         self.running = True
#
#         # Create a TCP socket
#         server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#
#         # Set socket timeout to handle accept() properly
#         server_socket.settimeout(1)
#
#         # Bind the socket to the port
#         server_address = ('', self.port)
#         try:
#             server_socket.bind(server_address)
#             logging.info(f"Server started on port {self.port}")
#         except Exception as e:
#             logging.error(f"Failed to bind to port {self.port}: {e}")
#             sys.exit(1)
#
#         # Listen for incoming connections
#         server_socket.listen(20)  # Increased backlog for more connections
#         logging.info("Waiting for connections...")
#
#         # Set up signal handler for graceful shutdown
#         def signal_handler(sig, frame):
#             logging.info("Shutting down server...")
#             self.running = False
#             server_socket.close()
#             sys.exit(0)
#
#         signal.signal(signal.SIGINT, signal_handler)
#         signal.signal(signal.SIGTERM, signal_handler)
#
#         # Main server loop
#         try:
#             while self.running:
#                 try:
#                     client_socket, address = server_socket.accept()
#                     # Set timeout for client socket
#                     client_socket.settimeout(30)  # 30 second timeout
#
#                     # Create thread for each client
#                     client_thread = threading.Thread(
#                         target=self.handle_client,
#                         args=(client_socket, address)
#                     )
#                     client_thread.daemon = True
#                     client_thread.start()
#
#                     if self.verbose:
#                         logging.debug(f"Started thread for client {address}")
#
#                 except socket.timeout:
#                     # Expected due to server socket timeout
#                     continue
#                 except OSError as e:
#                     if self.running:
#                         logging.error(f"Error accepting connection: {e}")
#                     break
#                 except Exception as e:
#                     logging.error(f"Unexpected error: {e}")
#                     continue
#
#         except KeyboardInterrupt:
#             logging.info("Server interrupted by user")
#         finally:
#             self.running = False
#             server_socket.close()
#             logging.info("Server shutdown complete")
#
#     def daemonize(self):
#         """Daemonize the current process."""
#         try:
#             pid = os.fork()
#             if pid > 0:
#                 sys.exit(0)
#         except OSError as e:
#             sys.stderr.write(f"Fork #1 failed: {e}\n")
#             sys.exit(1)
#
#         os.chdir("/")
#         os.setsid()
#         os.umask(0)
#
#         try:
#             pid = os.fork()
#             if pid > 0:
#                 sys.exit(0)
#         except OSError as e:
#             sys.stderr.write(f"Fork #2 failed: {e}\n")
#             sys.exit(1)
#
#         sys.stdout.flush()
#         sys.stderr.flush()
#         si = open(os.devnull, 'r')
#         so = open(os.devnull, 'a+')
#         se = open(os.devnull, 'a+')
#
#         os.dup2(si.fileno(), sys.stdin.fileno())
#         os.dup2(so.fileno(), sys.stdout.fileno())
#         os.dup2(se.fileno(), sys.stderr.fileno())
#
#
# def main():
#     """Parse command line arguments and start server."""
#     parser = argparse.ArgumentParser(
#         description="Socket Server with command line arguments support",
#         add_help=False
#     )
#
#     parser.add_argument(
#         "-p", "--port",
#         type=int,
#         default=5001,
#         help="TCP port that the server listens on (default: 5001)"
#     )
#
#     parser.add_argument(
#         "-v", "--verbose",
#         action="store_true",
#         help="give more detailed output"
#     )
#
#     parser.add_argument(
#         "-d", "--daemon",
#         action="store_true",
#         help="run the server as a daemon"
#     )
#
#     parser.add_argument(
#         "-h", "--help",
#         action="help",
#         default=argparse.SUPPRESS,
#         help="display this help message and exit"
#     )
#
#     args = parser.parse_args()
#
#     # Validate port number
#     if not (0 < args.port <= 65535):
#         print("Error: Port must be between 1 and 65535", file=sys.stderr)
#         sys.exit(1)
#
#     # Create and run server
#     server = SocketServer(port=args.port, verbose=args.verbose, daemon=args.daemon)
#     server.run()
#
#
# if __name__ == "__main__":
#     main()
