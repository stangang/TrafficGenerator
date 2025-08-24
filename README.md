# Traffic Generator

A Python-based traffic generation tool for testing network servers with configurable workload characteristics and Poisson process-based flow arrival patterns.

## Features

- **Multi-process Architecture**: Uses multiple worker processes to generate concurrent traffic
- **Poisson Process**: Flow arrival intervals follow exponential distribution for realistic traffic patterns
- **Configurable Workloads**: Support for custom flow size distributions and server configurations
- **Bandwidth Control**: Precisely control target bandwidth with real-time accuracy monitoring
- **Multiple Server Support**: Distribute traffic across multiple backend servers
- **Detailed Statistics**: Comprehensive logging and performance metrics

## Installation

```bash
git clone <repository-url>
cd traffic-generator
```

### Dependencies

- Python 3.6+
- No external dependencies required (uses standard library only)

## Configuration

### Configuration File Format

Create a configuration file with the following structure:

```
# Server configurations (multiple servers supported)
server 192.168.1.100 5001
server 192.168.1.101 5001

# Flow size distribution file (absolute path)
req_size_dist /path/to/flow_size_distribution.txt

# DSCP priorities (optional, read but not used)
dscp 0
dscp 10
dscp 18

# Client program path (optional)
client_path python3 socket_client.py
```

### Flow Size Distribution File

Create a flow size distribution file with cumulative probabilities:

```
# Size(Bytes) CumulativeProbability
1024 0.1
10240 0.3
102400 0.6
1048576 0.8
10485760 0.9
104857600 0.95
1073741824 1.0
```

## Usage

### Basic Commands

```bash
# Generate 100 flows with 50 Mbps bandwidth
python3 traffic_generator.py -c config.txt -n 100 -b 50 -v

# Generate traffic for 30 seconds with 100 Mbps bandwidth
python3 traffic_generator.py -c config.txt -t 30 -b 100 -v

# Use specific random seed for reproducible results
python3 traffic_generator.py -c config.txt -n 50 -b 25 -s 12345 -v

# Save results to custom log file
python3 traffic_generator.py -c config.txt -t 60 -b 200 -l my_results.txt
```

### Command Line Arguments

| Argument | Description | Required | Default |
|----------|-------------|----------|---------|
| `-b, --bandwidth` | Target bandwidth in Mbps | Yes | - |
| `-c, --config` | Configuration file path | Yes | - |
| `-n, --number` | Number of flows to generate | Either `-n` or `-t` | - |
| `-t, --time` | Time in seconds to generate flows | Either `-n` or `-t` | - |
| `-l, --log` | Output log file | No | `flows.txt` |
| `-s, --seed` | Random seed | No | Current time |
| `-r, --result` | Result parser script | No | - |
| `-v, --verbose` | Enable verbose output | No | False |
| `-h, --help` | Show help message | No | - |

## Output

### Log File Format

The tool generates a CSV file with the following columns:

```
flow_id,size_bytes,size_kb,server,start_time,end_time,completion_time,throughput_mbps,success,returncode,error
```

### Console Output

The tool provides real-time statistics including:

- Generation progress
- Worker process status
- Queue sizes
- Bandwidth accuracy
- Success/failure rates

Example output:
```
2024-01-15 10:30:45 - INFO - Started 4 worker processes...
2024-01-15 10:30:45 - INFO - Worker 1 started successfully (PID: 12345)
2024-01-15 10:30:45 - INFO - Worker 2 started successfully (PID: 12346)
2024-01-15 10:30:45 - INFO - Worker 3 started successfully (PID: 12347)
2024-01-15 10:30:45 - INFO - Worker 4 started successfully (PID: 12348)
2024-01-15 10:30:45 - INFO - Generated 10 flows
2024-01-15 10:30:46 - INFO - Progress: 50 generated, 45 completed, 2 failed
```

### Final Statistics

After completion, the tool displays comprehensive statistics:

```
============================================================
TRAFFIC GENERATION SUMMARY
============================================================
Total time:          62.34 seconds
Generated flows:     150
Completed flows:     148
Failed flows:        2
Success rate:        98.7%
Total data:          1.234 GB
============================================================
BANDWIDTH STATISTICS
============================================================
Actual bandwidth:    95.42 Mbps
Target bandwidth:    100.00 Mbps
Bandwidth accuracy:  95.4%
============================================================
```

## Architecture

### Process Model

```
Main Process
├── Task Queue (Multiprocessing.Queue)
├── Result Queue (Multiprocessing.Queue)
└── Worker Processes (4-8 instances)
    └── Client Process (subprocess.Popen)
        └── Network Server
```

### Key Components

1. **TrafficGenerator**: Main controller class
2. **Worker Processes**: Handle individual flow generation
3. **Queue System**: Inter-process communication
4. **Client Interface**: Communicates with backend servers

## Performance Considerations

### Resource Usage

- **Memory**: Each worker process uses ~10-20MB RAM
- **CPU**: Minimal CPU usage (I/O bound)
- **Network**: Limited by target bandwidth setting

### Optimization Tips

1. **Worker Count**: Use 2-4 workers per CPU core
2. **Queue Size**: Keep queue sizes reasonable (50-100)
3. **Flow Size**: Larger flows reduce overhead
4. **Timeout Settings**: Adjust based on network latency

## Error Handling

The tool includes comprehensive error handling:

- **Process Monitoring**: Automatic worker health checks
- **Retry Mechanism**: 3 retries for failed flows
- **Queue Management**: Timeout handling for full queues
- **Graceful Shutdown**: Proper cleanup on interruption

## Customization

### Extending Functionality

1. **Custom Client**: Modify `client_path` in config to use different client software
2. **Result Parsing**: Use `-r` option with custom parser script
3. **Distribution Models**: Modify flow size distribution file format
4. **Server Selection**: Implement different server selection algorithms

### Example Custom Client

```python
#!/usr/bin/env python3
# custom_client.py
import argparse
import socket
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', required=True)
    parser.add_argument('-p', '--port', type=int, required=True)
    parser.add_argument('-v', '--size', type=int, required=True)
    args = parser.parse_args()
    
    # Custom client implementation
    # ...

if __name__ == '__main__':
    main()
```

## Troubleshooting

### Common Issues

1. **Low Completion Rate**: Check server availability and network connectivity
2. **High Failure Rate**: Verify client program path and permissions
3. **Queue Full Errors**: Reduce generation rate or increase queue size
4. **Worker Process Failures**: Check system resource limits

### Debug Mode

Use verbose mode for detailed debugging:

```bash
python3 traffic_generator.py -c config.txt -n 10 -b 10 -v
```

## License

This project is provided as-is for educational and testing purposes.

## Support

For issues and questions, please check:
1. Server connectivity and firewall settings
2. Client program functionality
3. System resource availability
4. Configuration file syntax