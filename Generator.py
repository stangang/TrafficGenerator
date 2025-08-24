# #!/usr/bin/env python3
# """
# Traffic Generator with configurable workload characteristics.
# Uses multi-process approach by invoking client program.
# """
#
# import argparse
# import sys
# import time
# import random
# import logging
# import subprocess
# import multiprocessing
# import math
# import os
# import signal
# from datetime import datetime
# from collections import defaultdict
#
#
# class TrafficGenerator:
#     def __init__(self, config_file, bandwidth, seed=None, verbose=False):
#         self.config_file = config_file
#         self.bandwidth = bandwidth
#         self.verbose = verbose
#         self.completed_flows = []
#         self.server_index = 0
#         self.flow_counter = 0
#
#         # Set random seed
#         if seed is None:
#             seed = int(time.time())
#         random.seed(seed)
#         self.seed = seed
#
#         if verbose:
#             logging.info(f"Using random seed: {seed}")
#
#         # Load configuration
#         self.config = self.load_config(config_file)
#         self.validate_config()
#         self.flow_size_distribution = self.load_flow_size_distribution()
#
#     def load_config(self, config_file):
#         config = {
#             'servers': [],
#             'req_size_dist': None,
#             'dscp': [],
#             'client_path': 'python3 Client.py'
#         }
#
#         try:
#             with open(config_file, 'r') as f:
#                 for line in f:
#                     line = line.strip()
#                     if not line or line.startswith('#'):
#                         continue
#                     parts = line.split()
#                     if len(parts) < 2:
#                         continue
#
#                     key = parts[0].lower()
#                     value = parts[1:]
#
#                     if key == 'server':
#                         if len(value) >= 2:
#                             try:
#                                 config['servers'].append((value[0], int(value[1])))
#                             except ValueError:
#                                 pass
#                     elif key == 'req_size_dist':
#                         config['req_size_dist'] = ' '.join(value)
#                     elif key == 'dscp':
#                         if value and value[0].isdigit():
#                             config['dscp'].append(int(value[0]))
#                     elif key == 'client_path':
#                         config['client_path'] = ' '.join(value)
#
#             return config
#         except Exception as e:
#             logging.error(f"Error loading config: {e}")
#             sys.exit(1)
#
#     def generate_poisson_interval(self, lambda_rate):
#         """
#         Generate inter-arrival time using Poisson process (exponential distribution).
#
#         Args:
#             lambda_rate: Average arrival rate (flows per second)
#
#         Returns:
#             Inter-arrival time in seconds
#         """
#         if lambda_rate <= 0:
#             return float('inf')
#
#         # Generate exponential random variable with mean 1/lambda
#         # Using the inverse transform method: F^{-1}(U) = -ln(1-U)/lambda
#         u = random.random()
#         while u == 0:  # Avoid log(0)
#             u = random.random()
#
#         interval = -math.log(1 - u) / lambda_rate
#         return interval
#
#     def load_flow_size_distribution(self):
#         if not self.config['req_size_dist']:
#             logging.error("No flow size distribution file specified")
#             sys.exit(1)
#
#         distribution = []
#         try:
#             with open(self.config['req_size_dist'], 'r') as f:
#                 for line in f:
#                     line = line.strip()
#                     if not line or line.startswith('#'):
#                         continue
#                     parts = line.split()
#                     if len(parts) >= 2:
#                         try:
#                             size_bytes = float(parts[0])
#                             cumulative_prob = float(parts[1])
#                             if 0 <= cumulative_prob <= 1:
#                                 distribution.append((size_bytes, cumulative_prob))
#                         except ValueError:
#                             pass
#
#             distribution.sort(key=lambda x: x[1])
#             return distribution
#         except Exception as e:
#             logging.error(f"Error loading distribution: {e}")
#             sys.exit(1)
#
#     def validate_config(self):
#         if not self.config['servers']:
#             logging.error("At least one server configuration is required")
#             sys.exit(1)
#         if not self.config['req_size_dist']:
#             logging.error("Flow size distribution file is required")
#             sys.exit(1)
#
#     def generate_flow_size_bytes(self):
#         if not self.flow_size_distribution:
#             return 1024 * 1024
#
#         rand_val = random.random()
#         for size_bytes, cumulative_prob in self.flow_size_distribution:
#             if rand_val <= cumulative_prob:
#                 return max(1, size_bytes)
#         return max(1, self.flow_size_distribution[-1][0])
#
#     def get_next_server(self):
#         server = self.config['servers'][self.server_index]
#         self.server_index = (self.server_index + 1) % len(self.config['servers'])
#         return server
#
#     def calculate_bandwidth_stats(self, total_time):
#         """Calculate bandwidth statistics."""
#         if not self.completed_flows:
#             return 0.0, 0.0, 0.0
#
#         successful_flows = [f for f in self.completed_flows if f.get('success', False)]
#         if not successful_flows:
#             return 0.0, 0.0, 0.0
#
#         total_bytes = sum(flow.get('size_bytes', 0) for flow in successful_flows)
#
#         # Calculate actual bandwidth in Mbps
#         if total_time > 0:
#             actual_bandwidth = (total_bytes * 8) / (total_time * 1000000)  # Mbps
#         else:
#             actual_bandwidth = 0.0
#
#         target_bandwidth = self.bandwidth
#
#         # Calculate bandwidth accuracy percentage
#         if target_bandwidth > 0:
#             bandwidth_accuracy = (actual_bandwidth / target_bandwidth) * 100
#         else:
#             bandwidth_accuracy = 0.0
#
#         return actual_bandwidth, target_bandwidth, bandwidth_accuracy
#
#     def print_bandwidth_stats(self, total_time):
#         """Print bandwidth statistics."""
#         actual_bw, target_bw, accuracy = self.calculate_bandwidth_stats(total_time)
#
#         logging.info("=" * 60)
#         logging.info("BANDWIDTH STATISTICS")
#         logging.info("=" * 60)
#         logging.info(f"Actual bandwidth:   {actual_bw:.2f} Mbps")
#         logging.info(f"Target bandwidth:   {target_bw:.2f} Mbps")
#         logging.info(f"Bandwidth accuracy: {accuracy:.1f}%")
#
#         if accuracy < 80:
#             logging.warning("Bandwidth accuracy is below 80% - consider adjusting parameters")
#         elif accuracy > 120:
#             logging.warning("Bandwidth accuracy exceeds 120% - actual bandwidth higher than target")
#
#         logging.info("=" * 60)
#
#
# def run_client_process(config, server, flow_id, flow_size_bytes, verbose):
#     """Run client program as a subprocess."""
#     server_ip, server_port = server
#     flow_size_kb = max(1, int(flow_size_bytes / 1024))
#     client_cmd = config['client_path'].split()
#
#     client_cmd.extend(['-s', server_ip, '-p', str(server_port), '-v', str(flow_size_kb)])
#
#     if verbose:
#         logging.debug(f"Running client: {' '.join(client_cmd)}")
#
#     start_time = time.time()
#
#     try:
#         process = subprocess.Popen(
#             client_cmd,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             text=True
#         )
#
#         stdout, stderr = process.communicate(timeout=300)
#         end_time = time.time()
#         completion_time = end_time - start_time
#
#         success = process.returncode == 0
#         throughput = 0
#
#         if success and stdout:
#             for line in stdout.split('\n'):
#                 if 'Average speed:' in line:
#                     try:
#                         throughput = float(line.split(':')[1].strip().split()[0])
#                     except (IndexError, ValueError):
#                         pass
#
#         return {
#             'id': flow_id,
#             'size_bytes': flow_size_bytes,
#             'size_kb': flow_size_kb,
#             'server': f"{server_ip}:{server_port}",
#             'start_time': start_time,
#             'end_time': end_time,
#             'completion_time': completion_time,
#             'throughput_mbps': throughput,
#             'success': success,
#             'returncode': process.returncode,
#             'stdout': stdout,
#             'stderr': stderr
#         }
#
#     except subprocess.TimeoutExpired:
#         end_time = time.time()
#         return {
#             'id': flow_id,
#             'size_bytes': flow_size_bytes,
#             'size_kb': flow_size_kb,
#             'server': f"{server_ip}:{server_port}",
#             'start_time': start_time,
#             'end_time': end_time,
#             'completion_time': end_time - start_time,
#             'throughput_mbps': 0,
#             'success': False,
#             'returncode': -1,
#             'stdout': '',
#             'stderr': 'Process timeout'
#         }
#     except Exception as e:
#         end_time = time.time()
#         return {
#             'id': flow_id,
#             'size_bytes': flow_size_bytes,
#             'size_kb': flow_size_kb,
#             'server': f"{server_ip}:{server_port}",
#             'start_time': start_time,
#             'end_time': end_time,
#             'completion_time': end_time - start_time,
#             'throughput_mbps': 0,
#             'success': False,
#             'returncode': -1,
#             'stdout': '',
#             'stderr': str(e)
#         }
#
#
# def worker_process(task_queue, result_queue, config, verbose):
#     """Worker process that handles flow generation tasks."""
#     while True:
#         try:
#             task = task_queue.get(timeout=5)
#             if task is None:
#                 break
#
#             flow_id, flow_size_bytes, server = task
#             result = run_client_process(config, server, flow_id, flow_size_bytes, verbose)
#             result_queue.put(result)
#
#         except multiprocessing.queues.Empty:
#             break
#         except Exception as e:
#             logging.error(f"Worker process error: {e}")
#
#
# def generate_flows(generator, count=None, duration=None):
#     """Generate flows using multiple processes with proper synchronization."""
#     if count:
#         logging.info(f"Generating {count} flows")
#         total_flows = count
#     else:
#         logging.info(f"Generating flows for {duration} seconds")
#         total_flows = float('inf')
#
#     # Calculate target flow rate
#     avg_flow_size = sum(size for size, prob in generator.flow_size_distribution) / len(generator.flow_size_distribution)
#     avg_flow_size_kb = avg_flow_size / 1024
#     target_rate = (generator.bandwidth * 1000) / (avg_flow_size_kb * 8)
#
#     logging.info(f"Target flow rate: {target_rate:.2f} flows/sec")
#     logging.info(f"Average flow size: {avg_flow_size / 1024 / 1024:.2f} MB")
#
#     # Create queues and processes
#     task_queue = multiprocessing.Queue()
#     result_queue = multiprocessing.Queue()
#
#     num_workers = min(multiprocessing.cpu_count(), 24)
#     workers = []
#
#     for _ in range(num_workers):
#         p = multiprocessing.Process(
#             target=worker_process,
#             args=(task_queue, result_queue, generator.config, generator.verbose)
#         )
#         p.daemon = True
#         p.start()
#         workers.append(p)
#
#     start_time = time.time()
#     end_time = start_time + duration if duration else float('inf')
#     flow_id = 0
#     next_flow_time = start_time
#
#     # Statistics for Poisson process
#     arrival_times = []
#     inter_arrival_times = []
#
#     try:
#         while (flow_id < total_flows if count else time.time() < end_time):
#             current_time = time.time()
#
#             if current_time >= next_flow_time:
#                 flow_size_bytes = generator.generate_flow_size_bytes()
#                 server = generator.get_next_server()
#                 flow_id += 1
#
#                 # Record arrival time for statistics
#                 arrival_times.append(current_time)
#                 if len(arrival_times) > 1:
#                     inter_arrival_times.append(arrival_times[-1] - arrival_times[-2])
#
#                 try:
#                     task_queue.put((flow_id, flow_size_bytes, server), timeout=10)
#                     if generator.verbose:
#                         size_mb = flow_size_bytes / (1024 * 1024)
#                         logging.debug(f"Queued flow {flow_id}: {size_mb:.2f} MB to {server[0]}:{server[1]}")
#                 except multiprocessing.queues.Full:
#                     logging.warning("Task queue full, waiting...")
#                     time.sleep(1)
#                     continue
#
#                 # Generate next arrival time using Poisson process
#                 interval = generator.generate_poisson_interval(target_rate)
#                 next_flow_time = current_time + interval
#
#             # Check for results and process them
#             while not result_queue.empty():
#                 try:
#                     result = result_queue.get_nowait()
#                     generator.completed_flows.append(result)
#                 except:
#                     break
#
#             # Sleep briefly to avoid busy waiting
#             sleep_time = max(0, min(next_flow_time - current_time, 0.1))
#             time.sleep(sleep_time)
#
#     except KeyboardInterrupt:
#         logging.info("Traffic generation interrupted")
#     finally:
#         # Signal workers to stop
#         for _ in range(num_workers):
#             try:
#                 task_queue.put(None, timeout=5)
#             except:
#                 pass
#
#         # Wait for workers to finish current tasks
#         for p in workers:
#             p.join(timeout=30)
#             if p.is_alive():
#                 p.terminate()
#
#         # Process any remaining results
#         while not result_queue.empty():
#             try:
#                 result = result_queue.get_nowait()
#                 generator.completed_flows.append(result)
#             except:
#                 logging.info("Traffic generation interrupted")
#                 break
#
#         generator.flow_counter = flow_id
#
#         # Print Poisson process statistics
#         if inter_arrival_times:
#             avg_interval = sum(inter_arrival_times) / len(inter_arrival_times)
#             min_interval = min(inter_arrival_times)
#             max_interval = max(inter_arrival_times)
#             actual_lambda = 1 / avg_interval if avg_interval > 0 else 0
#
#             logging.info("=" * 60)
#             logging.info("POISSON PROCESS STATISTICS:")
#             logging.info("=" * 60)
#             logging.info(f"  Target λ:        {target_rate:.3f} flows/sec")
#             logging.info(f"  Actual λ:        {actual_lambda:.3f} flows/sec")
#             logging.info(f"  Mean interval:   {avg_interval:.3f} seconds")
#             logging.info(f"  Min interval:    {min_interval:.3f} seconds")
#             logging.info(f"  Max interval:    {max_interval:.3f} seconds")
#             logging.info(f"  Total flows:     {flow_id}")
#
#
# def setup_logging(verbose=False):
#     level = logging.DEBUG if verbose else logging.INFO
#     logging.basicConfig(
#         level=level,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         datefmt='%Y-%m-%d %H:%M:%S'
#     )
#
#
# def main():
#     parser = argparse.ArgumentParser(description="Traffic Generator", add_help=False)
#     parser.add_argument("-b", "--bandwidth", type=float, required=True, help="Bandwidth in Mbps")
#     parser.add_argument("-c", "--config", type=str, required=True, help="Configuration file")
#     group = parser.add_mutually_exclusive_group(required=True)
#     group.add_argument("-n", "--number", type=int, help="Number of requests")
#     group.add_argument("-t", "--time", type=float, help="Time in seconds")
#     parser.add_argument("-l", "--log", type=str, default="flows.txt", help="Log file")
#     parser.add_argument("-s", "--seed", type=int, help="Random seed")
#     parser.add_argument("-r", "--result", type=str, help="Result parser script")
#     parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
#     parser.add_argument("-h", "--help", action="help", help="Show help")
#
#     args = parser.parse_args()
#
#     if args.bandwidth <= 0:
#         print("Error: Bandwidth must be positive", file=sys.stderr)
#         sys.exit(1)
#
#     setup_logging(args.verbose)
#
#     try:
#         generator = TrafficGenerator(
#             config_file=args.config,
#             bandwidth=args.bandwidth,
#             seed=args.seed,
#             verbose=args.verbose
#         )
#     except Exception as e:
#         logging.error(f"Failed to initialize: {e}")
#         sys.exit(1)
#
#     start_time = time.time()
#
#     try:
#         if args.number:
#             generate_flows(generator, count=args.number)
#         else:
#             generate_flows(generator, duration=args.time)
#
#         # Final wait for completion
#         time.sleep(3)
#
#     except Exception as e:
#         logging.error(f"Error during generation: {e}")
#
#     end_time = time.time()
#     total_time = end_time - start_time
#
#     # Print detailed statistics
#     logging.info("=" * 60)
#     logging.info("TRAFFIC GENERATION SUMMARY")
#     logging.info("=" * 60)
#     logging.info(f"Total time:          {total_time:.2f} seconds")
#     logging.info(f"Generated flows:     {generator.flow_counter}")
#     logging.info(f"Completed flows:     {len(generator.completed_flows)}")
#
#     successful_flows = [f for f in generator.completed_flows if f.get('success', False)]
#     failed_flows = [f for f in generator.completed_flows if not f.get('success', True)]
#
#     logging.info(f"Successful flows:    {len(successful_flows)}")
#     logging.info(f"Failed flows:        {len(failed_flows)}")
#     logging.info(f"Success rate:        {len(successful_flows) / max(1, len(generator.completed_flows)) * 100:.1f}%")
#
#     if successful_flows:
#         total_bytes = sum(flow.get('size_bytes', 0) for flow in successful_flows)
#         total_data_gb = total_bytes / (1024 * 1024 * 1024)
#         logging.info(f"Total data:          {total_data_gb:.3f} GB")
#
#         # Calculate and display bandwidth statistics
#         generator.print_bandwidth_stats(total_time)
#
#         # Server statistics
#         server_stats = defaultdict(lambda: {'count': 0, 'total_bytes': 0})
#         for flow in successful_flows:
#             server = flow.get('server', 'unknown')
#             server_stats[server]['count'] += 1
#             server_stats[server]['total_bytes'] += flow.get('size_bytes', 0)
#
#         logging.info("SERVER STATISTICS:")
#         for server, stats in sorted(server_stats.items()):
#             data_gb = stats['total_bytes'] / (1024 * 1024 * 1024)
#             logging.info(f"  {server}: {stats['count']} flows, {data_gb:.3f} GB")
#
#     else:
#         logging.warning("No successful flows completed")
#         # Still show bandwidth statistics (will be 0)
#         generator.print_bandwidth_stats(total_time)
#
#     logging.info("=" * 60)
#
#     # Save results
#     try:
#         with open(args.log, 'w') as f:
#             f.write(
#                 "flow_id,size_bytes,size_kb,server,start_time,end_time,completion_time,throughput_mbps,success,returncode,error\n")
#             for flow in generator.completed_flows:
#                 error_msg = flow.get('stderr', '').replace(',', ';').replace('\n', '|')
#                 f.write(f"{flow['id']},{flow.get('size_bytes', 0)},{flow.get('size_kb', 0)},"
#                         f"'{flow.get('server', 'unknown')}',{flow.get('start_time', 0)},"
#                         f"{flow.get('end_time', 0)},{flow.get('completion_time', 0)},"
#                         f"{flow.get('throughput_mbps', 0)},{int(flow.get('success', False))},"
#                         f"{flow.get('returncode', -1)},{error_msg}\n")
#         logging.info(f"Results saved to {args.log}")
#     except Exception as e:
#         logging.error(f"Error saving results: {e}")
#
#     if args.result:
#         try:
#             result = subprocess.run(['python3', args.result, args.log],
#                                     capture_output=True, text=True, timeout=30)
#             if result.returncode == 0:
#                 logging.info("Result parsing completed")
#                 if result.stdout:
#                     logging.info(f"Parser output:\n{result.stdout}")
#             else:
#                 logging.error(f"Result parsing failed: {result.stderr}")
#         except Exception as e:
#             logging.error(f"Error parsing results: {e}")
#
# if __name__ == "__main__":
#     multiprocessing.set_start_method('spawn')
#     main()
#

# !/usr/bin/env python3
"""
Traffic Generator with fixed concurrency issues.
"""

import argparse
import sys
import time
import random
import logging
import subprocess
import multiprocessing
import os
import math
import queue
from collections import defaultdict


class TrafficGenerator:
    def __init__(self, config_file, bandwidth, seed=None, verbose=False):
        self.config_file = config_file
        self.bandwidth = bandwidth
        self.verbose = verbose
        self.completed_flows = []
        self.failed_flows = []
        self.server_index = 0
        self.flow_counter = 0

        if seed is None:
            seed = int(time.time())
        random.seed(seed)
        self.seed = seed

        if verbose:
            logging.info(f"Using random seed: {seed}")

        self.config = self.load_config(config_file)
        self.validate_config()
        self.flow_size_distribution = self.load_flow_size_distribution()

    def load_config(self, config_file):
        config = {
            'servers': [],
            'req_size_dist': None,
            'dscp': [],
            'client_path': 'python3 Client.py'
        }

        try:
            with open(config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split()
                    if len(parts) < 2:
                        continue

                    key = parts[0].lower()
                    value = parts[1:]

                    if key == 'server':
                        if len(value) >= 2:
                            try:
                                config['servers'].append((value[0], int(value[1])))
                            except ValueError:
                                pass
                    elif key == 'req_size_dist':
                        config['req_size_dist'] = ' '.join(value)
                    elif key == 'dscp':
                        if value and value[0].isdigit():
                            config['dscp'].append(int(value[0]))
                    elif key == 'client_path':
                        config['client_path'] = ' '.join(value)

            return config
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            sys.exit(1)

    def load_flow_size_distribution(self):
        if not self.config['req_size_dist']:
            logging.error("No flow size distribution file specified")
            sys.exit(1)

        distribution = []
        try:
            with open(self.config['req_size_dist'], 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            size_bytes = float(parts[0])
                            cumulative_prob = float(parts[1])
                            if 0 <= cumulative_prob <= 1:
                                distribution.append((size_bytes, cumulative_prob))
                        except ValueError:
                            pass

            distribution.sort(key=lambda x: x[1])
            return distribution
        except Exception as e:
            logging.error(f"Error loading distribution: {e}")
            sys.exit(1)

    def validate_config(self):
        if not self.config['servers']:
            logging.error("At least one server configuration is required")
            sys.exit(1)
        if not self.config['req_size_dist']:
            logging.error("Flow size distribution file is required")
            sys.exit(1)

    def generate_flow_size_bytes(self):
        if not self.flow_size_distribution:
            return 1024 * 1024

        rand_val = random.random()
        for size_bytes, cumulative_prob in self.flow_size_distribution:
            if rand_val <= cumulative_prob:
                return max(1, size_bytes)
        return max(1, self.flow_size_distribution[-1][0])

    def get_next_server(self):
        server = self.config['servers'][self.server_index]
        self.server_index = (self.server_index + 1) % len(self.config['servers'])
        return server

    def generate_poisson_interval(self, lambda_rate):
        if lambda_rate <= 0:
            return float('inf')

        u = random.random()
        while u == 0:
            u = random.random()

        interval = -math.log(1 - u) / lambda_rate
        return interval

    def calculate_bandwidth_stats(self, total_time):
        if not self.completed_flows:
            return 0.0, 0.0, 0.0

        successful_flows = [f for f in self.completed_flows if f.get('success', False)]
        if not successful_flows:
            return 0.0, 0.0, 0.0

        total_bytes = sum(flow.get('size_bytes', 0) for flow in successful_flows)

        if total_time > 0:
            actual_bandwidth = (total_bytes * 8) / (total_time * 1000000)
        else:
            actual_bandwidth = 0.0

        target_bandwidth = self.bandwidth

        if target_bandwidth > 0:
            bandwidth_accuracy = (actual_bandwidth / target_bandwidth) * 100
        else:
            bandwidth_accuracy = 0.0

        return actual_bandwidth, target_bandwidth, bandwidth_accuracy

    def print_bandwidth_stats(self, total_time):
        actual_bw, target_bw, accuracy = self.calculate_bandwidth_stats(total_time)

        logging.info("=" * 60)
        logging.info("BANDWIDTH STATISTICS")
        logging.info("=" * 60)
        logging.info(f"Actual bandwidth:   {actual_bw:.2f} Mbps")
        logging.info(f"Target bandwidth:   {target_bw:.2f} Mbps")
        logging.info(f"Bandwidth accuracy: {accuracy:.1f}%")
        logging.info("=" * 60)


def run_client_process(config, server, flow_id, flow_size_bytes, verbose):
    server_ip, server_port = server
    flow_size_kb = max(1, int(flow_size_bytes / 1024))
    client_cmd = config['client_path'].split()

    client_cmd.extend(['-s', server_ip, '-p', str(server_port), '-v', str(flow_size_kb)])

    if verbose:
        logging.debug(f"Running client: {' '.join(client_cmd)}")

    start_time = time.time()

    try:
        process = subprocess.Popen(
            client_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate(timeout=300)
        end_time = time.time()
        completion_time = end_time - start_time

        success = process.returncode == 0
        throughput = 0

        if success and stdout:
            for line in stdout.split('\n'):
                if 'Average speed:' in line:
                    try:
                        throughput = float(line.split(':')[1].strip().split()[0])
                    except (IndexError, ValueError):
                        pass

        return {
            'id': flow_id,
            'size_bytes': flow_size_bytes,
            'size_kb': flow_size_kb,
            'server': f"{server_ip}:{server_port}",
            'start_time': start_time,
            'end_time': end_time,
            'completion_time': completion_time,
            'throughput_mbps': throughput,
            'success': success,
            'returncode': process.returncode,
            'stdout': stdout,
            'stderr': stderr
        }

    except subprocess.TimeoutExpired:
        end_time = time.time()
        return {
            'id': flow_id,
            'size_bytes': flow_size_bytes,
            'size_kb': flow_size_kb,
            'server': f"{server_ip}:{server_port}",
            'start_time': start_time,
            'end_time': end_time,
            'completion_time': end_time - start_time,
            'throughput_mbps': 0,
            'success': False,
            'returncode': -1,
            'stdout': '',
            'stderr': 'Process timeout'
        }
    except Exception as e:
        end_time = time.time()
        return {
            'id': flow_id,
            'size_bytes': flow_size_bytes,
            'size_kb': flow_size_kb,
            'server': f"{server_ip}:{server_port}",
            'start_time': start_time,
            'end_time': end_time,
            'completion_time': end_time - start_time,
            'throughput_mbps': 0,
            'success': False,
            'returncode': -1,
            'stdout': '',
            'stderr': str(e)
        }


def worker_process(task_queue, result_queue, config, verbose, worker_id):
    """工作进程函数 - 修复版本"""
    logging.info(f"Worker {worker_id} started successfully")

    try:
        while True:
            try:
                # 获取任务，带超时防止永久阻塞
                task = task_queue.get(timeout=10.0)

                if task is None:  # 终止信号
                    logging.info(f"Worker {worker_id} received termination signal")
                    break

                flow_id, flow_size_bytes, server = task

                if verbose:
                    logging.debug(f"Worker {worker_id} processing flow {flow_id}")

                # 处理任务
                result = run_client_process(config, server, flow_id, flow_size_bytes, verbose)

                # 发送结果
                try:
                    result_queue.put(result, timeout=5.0)
                    if verbose:
                        status = "success" if result['success'] else "failed"
                        logging.debug(f"Worker {worker_id} flow {flow_id} {status}")
                except queue.Full:
                    logging.error(f"Worker {worker_id} result queue full")

            except queue.Empty:
                # 正常超时，继续等待
                continue
            except Exception as e:
                logging.error(f"Worker {worker_id} error: {e}")
                time.sleep(1)  # 错误后稍作休息

    except Exception as e:
        logging.error(f"Worker {worker_id} fatal error: {e}")
    finally:
        logging.info(f"Worker {worker_id} exiting")


def generate_flows(generator, count=None, duration=None):
    """生成流量的主函数 - 修复版本"""
    if count:
        logging.info(f"Generating {count} flows")
        total_flows = count
    else:
        logging.info(f"Generating flows for {duration} seconds")
        total_flows = float('inf')

    # 计算到达率
    avg_flow_size_bytes = sum(size for size, prob in generator.flow_size_distribution) / len(
        generator.flow_size_distribution)
    avg_flow_size_bits = avg_flow_size_bytes * 8
    lambda_rate = (generator.bandwidth * 1000000) / avg_flow_size_bits

    # 创建队列 - 使用合理的大小
    task_queue = multiprocessing.Queue(maxsize=100)
    result_queue = multiprocessing.Queue(maxsize=200)

    # 启动工作进程 - 关键修复：确保进程真正启动
    num_workers = min(multiprocessing.cpu_count(), 8)  # 保守的数量
    workers = []

    # Statistics for Poisson process
    arrival_times = []
    inter_arrival_times = []

    logging.info(f"Starting {num_workers} worker processes...")
    for i in range(num_workers):
        try:
            p = multiprocessing.Process(
                target=worker_process,
                args=(task_queue, result_queue, generator.config, generator.verbose, i + 1)
            )
            p.daemon = False  # 重要：不要设置为daemon
            p.start()
            workers.append(p)
            logging.info(f"Started worker {i + 1} (PID: {p.pid})")
            time.sleep(0.2)  # 稍微延迟，避免冲突
        except Exception as e:
            logging.error(f"Failed to start worker {i + 1}: {e}")

    if not workers:
        logging.error("No workers started, aborting")
        return

    start_time = time.time()
    end_time = start_time + duration if duration else float('inf')
    flow_id = 0
    next_arrival_time = start_time

    try:
        while (flow_id < total_flows if count else time.time() < end_time):
            current_time = time.time()

            # 生成新任务
            if current_time >= next_arrival_time and flow_id < total_flows:
                flow_size_bytes = generator.generate_flow_size_bytes()
                server = generator.get_next_server()

                # 放入任务队列
                try:
                    task_queue.put((flow_id, flow_size_bytes, server), timeout=2.0)
                    flow_id += 1

                    # Record arrival time for statistics
                    arrival_times.append(current_time)
                    if len(arrival_times) > 1:
                        inter_arrival_times.append(arrival_times[-1] - arrival_times[-2])

                    if generator.verbose and flow_id % 10 == 0:
                        logging.info(f"Generated {flow_id} flows")

                except queue.Full:
                    logging.warning("Task queue full, waiting...")
                    time.sleep(0.5)
                    continue

                # 生成下一个到达时间
                interval = generator.generate_poisson_interval(lambda_rate)
                next_arrival_time = current_time + interval

            # 处理结果 - 持续进行
            try:
                while True:
                    result = result_queue.get_nowait()
                    if result['success']:
                        generator.completed_flows.append(result)
                    else:
                        generator.failed_flows.append(result)
            except queue.Empty:
                pass

            # 检查工作进程状态
            alive_workers = [p for p in workers if p.is_alive()]
            if len(alive_workers) < num_workers:
                logging.warning(f"Only {len(alive_workers)}/{num_workers} workers alive")

            # 短暂休眠
            sleep_time = max(0.01, min(next_arrival_time - current_time, 0.1))
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.info("Traffic generation interrupted")
    except Exception as e:
        logging.error(f"Error in generation: {e}")
    finally:
        # 清理过程
        logging.info("Starting cleanup...")

        # 发送终止信号
        for _ in range(len(workers)):
            try:
                task_queue.put(None, timeout=2.0)
            except:
                pass

        # 等待工作进程结束
        timeout_time = time.time() + 10.0
        while time.time() < timeout_time and any(p.is_alive() for p in workers):
            # 处理剩余结果
            try:
                while True:
                    result = result_queue.get_nowait()
                    if result['success']:
                        generator.completed_flows.append(result)
                    else:
                        generator.failed_flows.append(result)
            except queue.Empty:
                pass
            time.sleep(0.1)

        # 强制终止残留进程
        for p in workers:
            if p.is_alive():
                p.terminate()
            p.join(timeout=2.0)

        generator.flow_counter = flow_id
        logging.info(f"Cleanup completed. Generated: {flow_id}, Completed: {len(generator.completed_flows)}")

        # Print Poisson process statistics
        if inter_arrival_times:
            avg_interval = sum(inter_arrival_times) / len(inter_arrival_times)
            min_interval = min(inter_arrival_times)
            max_interval = max(inter_arrival_times)
            actual_lambda = 1 / avg_interval if avg_interval > 0 else 0

            logging.info("=" * 60)
            logging.info("POISSON PROCESS STATISTICS:")
            logging.info("=" * 60)
            logging.info(f"  Target λ:        {lambda_rate:.3f} flows/sec")
            logging.info(f"  Actual λ:        {actual_lambda:.3f} flows/sec")
            logging.info(f"  Mean interval:   {avg_interval:.3f} seconds")
            logging.info(f"  Min interval:    {min_interval:.3f} seconds")
            logging.info(f"  Max interval:    {max_interval:.3f} seconds")
            logging.info(f"  Total flows:     {flow_id}")


def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    parser = argparse.ArgumentParser(description="Traffic Generator", add_help=False)
    parser.add_argument("-b", "--bandwidth", type=float, required=True, help="Bandwidth in Mbps")
    parser.add_argument("-c", "--config", type=str, required=True, help="Configuration file")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-n", "--number", type=int, help="Number of requests")
    group.add_argument("-t", "--time", type=float, help="Time in seconds")
    parser.add_argument("-l", "--log", type=str, default="flows.txt", help="Log file")
    parser.add_argument("-s", "--seed", type=int, help="Random seed")
    parser.add_argument("-r", "--result", type=str, help="Result parser script")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-h", "--help", action="help", help="Show help")

    args = parser.parse_args()

    if args.bandwidth <= 0:
        print("Error: Bandwidth must be positive", file=sys.stderr)
        sys.exit(1)

    setup_logging(args.verbose)

    try:
        generator = TrafficGenerator(
            config_file=args.config,
            bandwidth=args.bandwidth,
            seed=args.seed,
            verbose=args.verbose
        )
    except Exception as e:
        logging.error(f"Failed to initialize: {e}")
        sys.exit(1)

    start_time = time.time()

    try:
        if args.number:
            generate_flows(generator, count=args.number)
        else:
            generate_flows(generator, duration=args.time)

    except Exception as e:
        logging.error(f"Error during generation: {e}")

    end_time = time.time()
    total_time = end_time - start_time

    # 输出统计信息
    logging.info("=" * 60)
    logging.info("TRAFFIC GENERATION SUMMARY")
    logging.info("=" * 60)
    logging.info(f"Total time:          {total_time:.2f} seconds")
    logging.info(f"Generated flows:     {generator.flow_counter}")
    logging.info(f"Completed flows:     {len(generator.completed_flows)}")
    logging.info(f"Failed flows:        {len(generator.failed_flows)}")

    success_rate = len(generator.completed_flows) / max(1, generator.flow_counter) * 100
    logging.info(f"Success rate:        {success_rate:.1f}%")

    if generator.completed_flows:
        total_bytes = sum(flow.get('size_bytes', 0) for flow in generator.completed_flows)
        total_data_gb = total_bytes / (1024 * 1024 * 1024)
        logging.info(f"Total data:          {total_data_gb:.3f} GB")

        generator.print_bandwidth_stats(total_time)

    # 保存结果
    try:
        with open(args.log, 'w') as f:
            f.write(
                "flow_id,size_bytes,size_kb,server,start_time,end_time,completion_time,throughput_mbps,success,returncode,error\n")
            for flow in generator.completed_flows + generator.failed_flows:
                error_msg = flow.get('stderr', '').replace(',', ';').replace('\n', '|')
                f.write(f"{flow['id']},{flow.get('size_bytes', 0)},{flow.get('size_kb', 0)},"
                        f"'{flow.get('server', 'unknown')}',{flow.get('start_time', 0)},"
                        f"{flow.get('end_time', 0)},{flow.get('completion_time', 0)},"
                        f"{flow.get('throughput_mbps', 0)},{int(flow.get('success', False))},"
                        f"{flow.get('returncode', -1)},{error_msg}\n")
        logging.info(f"Results saved to {args.log}")
    except Exception as e:
        logging.error(f"Error saving results: {e}")

    if args.result:
        try:
            result = subprocess.run(['python3', args.result, args.log],
                                    capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                logging.info("Result parsing completed")
        except Exception as e:
            logging.error(f"Error parsing results: {e}")


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    main()
