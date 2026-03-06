#!/usr/bin/env python3
"""
CCPR Memory Metrics Analyzer
分析CCPR内存控制测试的metrics数据
"""

import os
import sys
import re
import csv
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("Warning: matplotlib not available, charts will not be generated")


class MetricsParser:
    """解析Prometheus格式的metrics"""
    
    METRIC_PATTERN = re.compile(
        r'^(\w+)(?:\{([^}]*)\})?\s+(\S+)(?:\s+(\d+))?$'
    )
    
    @staticmethod
    def parse_line(line: str) -> Optional[Tuple[str, Dict[str, str], float]]:
        """解析单行metric"""
        line = line.strip()
        if not line or line.startswith('#'):
            return None
            
        match = MetricsParser.METRIC_PATTERN.match(line)
        if not match:
            return None
            
        name = match.group(1)
        labels_str = match.group(2) or ""
        value = float(match.group(3))
        
        # 解析labels
        labels = {}
        if labels_str:
            for part in labels_str.split(','):
                if '=' in part:
                    k, v = part.split('=', 1)
                    labels[k.strip()] = v.strip().strip('"')
        
        return name, labels, value
    
    @staticmethod
    def parse_file(filepath: str) -> Dict[str, List[Tuple[Dict[str, str], float]]]:
        """解析metrics文件"""
        metrics = defaultdict(list)
        
        with open(filepath, 'r') as f:
            for line in f:
                result = MetricsParser.parse_line(line)
                if result:
                    name, labels, value = result
                    metrics[name].append((labels, value))
        
        return dict(metrics)


class CCPRMetricsAnalyzer:
    """CCPR Metrics分析器"""
    
    def __init__(self, results_dir: str):
        self.results_dir = results_dir
        self.metrics_files = self._find_metrics_files()
        self.timing_data = self._load_timing_data()
        self.monitor_data = self._load_monitor_data()
    
    def _find_metrics_files(self) -> Dict[str, str]:
        """查找所有metrics文件"""
        files = {}
        for filename in os.listdir(self.results_dir):
            if filename.startswith('metrics_') and filename.endswith('.txt'):
                label = filename[8:-4]  # Remove 'metrics_' and '.txt'
                files[label] = os.path.join(self.results_dir, filename)
        return files
    
    def _load_timing_data(self) -> List[Tuple[str, float]]:
        """加载timing数据"""
        timing_file = os.path.join(self.results_dir, 'timing.csv')
        if not os.path.exists(timing_file):
            return []
        
        data = []
        with open(timing_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 2:
                    data.append((row[0], float(row[1])))
        return data
    
    def _load_monitor_data(self) -> List[Dict]:
        """加载监控数据"""
        monitor_file = os.path.join(self.results_dir, 'metrics_monitor.csv')
        if not os.path.exists(monitor_file):
            return []
        
        data = []
        with open(monitor_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        return data
    
    def analyze_memory_usage(self) -> Dict:
        """分析内存使用情况"""
        memory_stats = {
            'initial': {},
            'peak': {},
            'final': {},
            'by_type': defaultdict(list)
        }
        
        for label, filepath in sorted(self.metrics_files.items()):
            metrics = MetricsParser.parse_file(filepath)
            
            # 获取内存metrics
            for name, values in metrics.items():
                if name == 'mo_ccpr_memory_bytes':
                    for labels, value in values:
                        mem_type = labels.get('type', 'unknown')
                        memory_stats['by_type'][mem_type].append({
                            'label': label,
                            'value': value
                        })
                        
                        if label == 'initial':
                            memory_stats['initial'][mem_type] = value
                        elif label == 'final':
                            memory_stats['final'][mem_type] = value
        
        # 计算峰值
        for mem_type, readings in memory_stats['by_type'].items():
            if readings:
                memory_stats['peak'][mem_type] = max(r['value'] for r in readings)
        
        return memory_stats
    
    def analyze_pool_efficiency(self) -> Dict:
        """分析对象池效率"""
        pool_stats = defaultdict(lambda: {'hit': 0, 'miss': 0})
        
        # 使用最终metrics
        if 'final' in self.metrics_files:
            metrics = MetricsParser.parse_file(self.metrics_files['final'])
            
            for name, values in metrics.items():
                if name == 'mo_ccpr_pool_total':
                    for labels, value in values:
                        pool_type = labels.get('type', 'unknown')
                        result = labels.get('result', 'unknown')
                        pool_stats[pool_type][result] = value
        
        # 计算命中率
        for pool_type in pool_stats:
            hit = pool_stats[pool_type]['hit']
            miss = pool_stats[pool_type]['miss']
            total = hit + miss
            pool_stats[pool_type]['hit_rate'] = hit / total if total > 0 else 0
        
        return dict(pool_stats)
    
    def analyze_job_performance(self) -> Dict:
        """分析Job性能"""
        job_stats = {}
        
        if 'final' in self.metrics_files:
            metrics = MetricsParser.parse_file(self.metrics_files['final'])
            
            # Job计数
            for name, values in metrics.items():
                if name == 'mo_ccpr_job_total':
                    for labels, value in values:
                        job_type = labels.get('type', 'unknown')
                        status = labels.get('status', 'unknown')
                        
                        if job_type not in job_stats:
                            job_stats[job_type] = {}
                        job_stats[job_type][status] = value
        
        return job_stats
    
    def generate_report(self) -> str:
        """生成分析报告"""
        memory_stats = self.analyze_memory_usage()
        pool_stats = self.analyze_pool_efficiency()
        job_stats = self.analyze_job_performance()
        
        report = []
        report.append("# CCPR Memory Metrics Analysis Report")
        report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Results Directory:** {self.results_dir}")
        
        # 内存使用分析
        report.append("\n## Memory Usage Analysis")
        report.append("\n### Memory by Type")
        report.append("\n| Type | Initial | Peak | Final |")
        report.append("|------|---------|------|-------|")
        
        all_types = set(memory_stats['initial'].keys()) | \
                    set(memory_stats['peak'].keys()) | \
                    set(memory_stats['final'].keys())
        
        for mem_type in sorted(all_types):
            initial = memory_stats['initial'].get(mem_type, 0)
            peak = memory_stats['peak'].get(mem_type, 0)
            final = memory_stats['final'].get(mem_type, 0)
            report.append(f"| {mem_type} | {self._format_bytes(initial)} | {self._format_bytes(peak)} | {self._format_bytes(final)} |")
        
        # 对象池效率
        report.append("\n## Object Pool Efficiency")
        report.append("\n| Pool Type | Hits | Misses | Hit Rate |")
        report.append("|-----------|------|--------|----------|")
        
        for pool_type, stats in sorted(pool_stats.items()):
            hit = stats.get('hit', 0)
            miss = stats.get('miss', 0)
            hit_rate = stats.get('hit_rate', 0)
            report.append(f"| {pool_type} | {int(hit)} | {int(miss)} | {hit_rate:.2%} |")
        
        # Job性能
        report.append("\n## Job Performance")
        report.append("\n| Job Type | Completed | Errors | Error Rate |")
        report.append("|----------|-----------|--------|------------|")
        
        for job_type, stats in sorted(job_stats.items()):
            completed = stats.get('completed', 0)
            errors = stats.get('error', 0)
            total = completed + errors
            error_rate = errors / total if total > 0 else 0
            report.append(f"| {job_type} | {int(completed)} | {int(errors)} | {error_rate:.2%} |")
        
        # 操作耗时
        if self.timing_data:
            report.append("\n## Operation Timing")
            report.append("\n| Operation | Duration (s) |")
            report.append("|-----------|--------------|")
            
            for op, duration in self.timing_data:
                report.append(f"| {op} | {duration:.2f} |")
        
        return '\n'.join(report)
    
    def generate_charts(self, output_dir: Optional[str] = None):
        """生成可视化图表"""
        if not HAS_MATPLOTLIB:
            print("matplotlib not available, skipping chart generation")
            return
        
        output_dir = output_dir or self.results_dir
        
        # 内存使用趋势图
        self._plot_memory_trend(output_dir)
        
        # 对象池命中率图
        self._plot_pool_hit_rate(output_dir)
        
        # 操作耗时图
        self._plot_timing(output_dir)
    
    def _plot_memory_trend(self, output_dir: str):
        """绘制内存使用趋势图"""
        if not self.monitor_data:
            return
        
        timestamps = []
        mem_total = []
        mem_object = []
        mem_decompress = []
        
        for row in self.monitor_data:
            try:
                timestamps.append(datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S'))
                mem_total.append(float(row.get('memory_total', 0)))
                mem_object.append(float(row.get('memory_object_content', 0)))
                mem_decompress.append(float(row.get('memory_decompress', 0)))
            except (ValueError, KeyError):
                continue
        
        if not timestamps:
            return
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(timestamps, [m / 1024 / 1024 for m in mem_total], label='Total', linewidth=2)
        ax.plot(timestamps, [m / 1024 / 1024 for m in mem_object], label='Object Content', linewidth=2)
        ax.plot(timestamps, [m / 1024 / 1024 for m in mem_decompress], label='Decompress Buffer', linewidth=2)
        
        ax.set_xlabel('Time')
        ax.set_ylabel('Memory (MB)')
        ax.set_title('CCPR Memory Usage Over Time')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'memory_trend.png'), dpi=150)
        plt.close()
        
        print(f"Memory trend chart saved to {output_dir}/memory_trend.png")
    
    def _plot_pool_hit_rate(self, output_dir: str):
        """绘制对象池命中率图"""
        pool_stats = self.analyze_pool_efficiency()
        
        if not pool_stats:
            return
        
        types = list(pool_stats.keys())
        hit_rates = [pool_stats[t].get('hit_rate', 0) * 100 for t in types]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(types, hit_rates, color=['#2ecc71', '#3498db', '#e74c3c', '#f39c12'])
        
        ax.set_xlabel('Pool Type')
        ax.set_ylabel('Hit Rate (%)')
        ax.set_title('Object Pool Hit Rates')
        ax.set_ylim(0, 100)
        
        # 添加数值标签
        for bar, rate in zip(bars, hit_rates):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                   f'{rate:.1f}%', ha='center', va='bottom')
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'pool_hit_rate.png'), dpi=150)
        plt.close()
        
        print(f"Pool hit rate chart saved to {output_dir}/pool_hit_rate.png")
    
    def _plot_timing(self, output_dir: str):
        """绘制操作耗时图"""
        if not self.timing_data:
            return
        
        operations = [t[0] for t in self.timing_data]
        durations = [t[1] for t in self.timing_data]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        bars = ax.barh(operations, durations, color='#3498db')
        
        ax.set_xlabel('Duration (seconds)')
        ax.set_title('Operation Timing')
        
        # 添加数值标签
        for bar, duration in zip(bars, durations):
            ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                   f'{duration:.2f}s', ha='left', va='center')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'timing.png'), dpi=150)
        plt.close()
        
        print(f"Timing chart saved to {output_dir}/timing.png")
    
    @staticmethod
    def _format_bytes(value: float) -> str:
        """格式化字节数"""
        if value == 0:
            return "0 B"
        
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        unit_idx = 0
        
        while value >= 1024 and unit_idx < len(units) - 1:
            value /= 1024
            unit_idx += 1
        
        return f"{value:.2f} {units[unit_idx]}"


def main():
    """主函数"""
    if len(sys.argv) < 2:
        # 查找最新的测试结果目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        result_dirs = [d for d in os.listdir(current_dir) if d.startswith('test_results_')]
        
        if not result_dirs:
            print("Usage: python analyze_metrics.py <results_directory>")
            print("No test results found in current directory.")
            sys.exit(1)
        
        results_dir = os.path.join(current_dir, sorted(result_dirs)[-1])
        print(f"Using latest results directory: {results_dir}")
    else:
        results_dir = sys.argv[1]
    
    if not os.path.isdir(results_dir):
        print(f"Error: Directory not found: {results_dir}")
        sys.exit(1)
    
    analyzer = CCPRMetricsAnalyzer(results_dir)
    
    # 生成报告
    report = analyzer.generate_report()
    report_file = os.path.join(results_dir, 'analysis_report.md')
    with open(report_file, 'w') as f:
        f.write(report)
    print(f"Report saved to: {report_file}")
    
    # 打印报告
    print("\n" + "="*60)
    print(report)
    print("="*60 + "\n")
    
    # 生成图表
    analyzer.generate_charts()
    
    # 导出JSON格式数据
    json_data = {
        'memory': analyzer.analyze_memory_usage(),
        'pools': analyzer.analyze_pool_efficiency(),
        'jobs': analyzer.analyze_job_performance(),
        'timing': analyzer.timing_data
    }
    
    # 处理defaultdict
    json_data['memory']['by_type'] = dict(json_data['memory']['by_type'])
    
    json_file = os.path.join(results_dir, 'analysis_data.json')
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)
    print(f"JSON data saved to: {json_file}")


if __name__ == '__main__':
    main()
