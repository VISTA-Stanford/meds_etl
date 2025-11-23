#!/usr/bin/env python
"""
Analyze Benchmark Results and Generate Reports

This script analyzes benchmark JSON files and generates:
- PDF plots showing performance metrics
- Markdown tables summarizing results
- Performance report document

Usage:
    python analyze_results.py --input runs/tuning_streaming_*.json --output_dir reports/
    python analyze_results.py --input runs/*.json --output_dir reports/ --format both
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import pandas as pd

# Set style for plots
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 10


def load_results(json_file):
    """Load results from JSON file"""
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data


def create_performance_plots(results_data, output_dir):
    """Create performance plots and save as PDFs"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    experiment_type = results_data.get('experiment_type', 'unknown')
    timestamp = results_data.get('timestamp', 'unknown')
    results = [r for r in results_data['results'] if r.get('success', False)]
    
    if not results:
        print(f"No successful results to plot for {experiment_type}")
        return []
    
    plots_created = []
    
    # 1. Overall Performance Bar Chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    experiment_ids = [r['experiment_id'] for r in results]
    elapsed_times = [r['elapsed_minutes'] for r in results]
    
    # Color by performance (green = fast, red = slow)
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(elapsed_times)))
    sorted_indices = np.argsort(elapsed_times)
    colors = [colors[np.where(sorted_indices == i)[0][0]] for i in range(len(elapsed_times))]
    
    bars = ax.bar(range(len(experiment_ids)), elapsed_times, color=colors)
    ax.set_xlabel('Experiment')
    ax.set_ylabel('Time (minutes)')
    ax.set_title(f'Benchmark Performance: {experiment_type}')
    ax.set_xticks(range(len(experiment_ids)))
    ax.set_xticklabels(experiment_ids, rotation=45, ha='right')
    
    # Add value labels on bars
    for i, (bar, time) in enumerate(zip(bars, elapsed_times)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{time:.1f}m',
                ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    plot_file = output_dir / f'performance_overall_{experiment_type}_{timestamp}.pdf'
    plt.savefig(plot_file, bbox_inches='tight')
    plt.close()
    plots_created.append(plot_file)
    print(f"  Created: {plot_file}")
    
    # 2. Stage Breakdown (if available)
    if results[0].get('stage_timings'):
        fig, ax = plt.subplots(figsize=(12, 6))
        
        stage1_times = [r.get('stage_timings', {}).get('stage1', 0) / 60 for r in results]
        stage2_times = [r.get('stage_timings', {}).get('stage2', 0) / 60 for r in results]
        
        x = np.arange(len(experiment_ids))
        width = 0.35
        
        ax.bar(x - width/2, stage1_times, width, label='Stage 1 (Transform)', color='#3498db')
        ax.bar(x + width/2, stage2_times, width, label='Stage 2 (Sort)', color='#e74c3c')
        
        ax.set_xlabel('Experiment')
        ax.set_ylabel('Time (minutes)')
        ax.set_title(f'Stage Breakdown: {experiment_type}')
        ax.set_xticks(x)
        ax.set_xticklabels(experiment_ids, rotation=45, ha='right')
        ax.legend()
        
        plt.tight_layout()
        plot_file = output_dir / f'performance_stages_{experiment_type}_{timestamp}.pdf'
        plt.savefig(plot_file, bbox_inches='tight')
        plt.close()
        plots_created.append(plot_file)
        print(f"  Created: {plot_file}")
    
    # 3. Parameter-specific plots
    if experiment_type == 'workers':
        plot_file = plot_workers_performance(results, output_dir, timestamp)
        if plot_file:
            plots_created.append(plot_file)
    
    elif experiment_type in ['workers_and_shards', 'full']:
        plot_file = plot_workers_shards_heatmap(results, output_dir, timestamp, experiment_type)
        if plot_file:
            plots_created.append(plot_file)
    
    elif experiment_type == 'shards':
        plot_file = plot_shards_performance(results, output_dir, timestamp)
        if plot_file:
            plots_created.append(plot_file)
    
    elif experiment_type == 'compression':
        plot_file = plot_compression_heatmap(results, output_dir, timestamp)
        if plot_file:
            plots_created.append(plot_file)
    
    return plots_created


def plot_workers_performance(results, output_dir, timestamp):
    """Plot worker count vs performance"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    workers = [r['params']['workers'] for r in results]
    times = [r['elapsed_minutes'] for r in results]
    
    ax.plot(workers, times, marker='o', linewidth=2, markersize=8, color='#2ecc71')
    ax.set_xlabel('Number of Workers')
    ax.set_ylabel('Time (minutes)')
    ax.set_title('Performance vs Worker Count')
    ax.grid(True, alpha=0.3)
    
    # Add throughput (relative speedup)
    baseline_time = max(times)
    for w, t in zip(workers, times):
        speedup = baseline_time / t
        ax.annotate(f'{speedup:.2f}x', xy=(w, t), xytext=(5, 5),
                   textcoords='offset points', fontsize=8)
    
    plt.tight_layout()
    plot_file = output_dir / f'workers_performance_{timestamp}.pdf'
    plt.savefig(plot_file, bbox_inches='tight')
    plt.close()
    print(f"  Created: {plot_file}")
    return plot_file


def plot_shards_performance(results, output_dir, timestamp):
    """Plot shard count vs performance"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    shards = [r['params']['shards'] for r in results]
    times = [r['elapsed_minutes'] for r in results]
    
    ax.plot(shards, times, marker='s', linewidth=2, markersize=8, color='#e74c3c')
    ax.set_xlabel('Number of Shards')
    ax.set_ylabel('Time (minutes)')
    ax.set_title('Performance vs Shard Count')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plot_file = output_dir / f'shards_performance_{timestamp}.pdf'
    plt.savefig(plot_file, bbox_inches='tight')
    plt.close()
    print(f"  Created: {plot_file}")
    return plot_file


def plot_workers_shards_heatmap(results, output_dir, timestamp, experiment_type):
    """Plot heatmap of workers vs shards"""
    # Extract unique workers and shards
    workers_vals = sorted(list(set(r['params']['workers'] for r in results)))
    shards_vals = sorted(list(set(r['params']['shards'] for r in results)))
    
    # Create matrix
    matrix = np.full((len(shards_vals), len(workers_vals)), np.nan)
    
    for r in results:
        w_idx = workers_vals.index(r['params']['workers'])
        s_idx = shards_vals.index(r['params']['shards'])
        matrix[s_idx, w_idx] = r['elapsed_minutes']
    
    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(matrix, cmap='RdYlGn_r', aspect='auto')
    
    # Set ticks
    ax.set_xticks(np.arange(len(workers_vals)))
    ax.set_yticks(np.arange(len(shards_vals)))
    ax.set_xticklabels(workers_vals)
    ax.set_yticklabels(shards_vals)
    
    ax.set_xlabel('Workers')
    ax.set_ylabel('Shards')
    ax.set_title('Performance Heatmap: Workers × Shards (minutes)')
    
    # Add text annotations
    for i in range(len(shards_vals)):
        for j in range(len(workers_vals)):
            if not np.isnan(matrix[i, j]):
                text = ax.text(j, i, f'{matrix[i, j]:.1f}',
                             ha="center", va="center", color="black", fontsize=9)
    
    plt.colorbar(im, ax=ax, label='Time (minutes)')
    plt.tight_layout()
    
    plot_file = output_dir / f'workers_shards_heatmap_{experiment_type}_{timestamp}.pdf'
    plt.savefig(plot_file, bbox_inches='tight')
    plt.close()
    print(f"  Created: {plot_file}")
    return plot_file


def plot_compression_heatmap(results, output_dir, timestamp):
    """Plot heatmap of compression combinations"""
    # Extract unique compression types
    run_comp_vals = sorted(list(set(r['params']['run_compression'] for r in results)))
    final_comp_vals = sorted(list(set(r['params']['final_compression'] for r in results)))
    
    # Create matrix
    matrix = np.full((len(final_comp_vals), len(run_comp_vals)), np.nan)
    
    for r in results:
        r_idx = run_comp_vals.index(r['params']['run_compression'])
        f_idx = final_comp_vals.index(r['params']['final_compression'])
        matrix[f_idx, r_idx] = r['elapsed_minutes']
    
    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(matrix, cmap='RdYlGn_r', aspect='auto')
    
    ax.set_xticks(np.arange(len(run_comp_vals)))
    ax.set_yticks(np.arange(len(final_comp_vals)))
    ax.set_xticklabels(run_comp_vals)
    ax.set_yticklabels(final_comp_vals)
    
    ax.set_xlabel('Run Compression')
    ax.set_ylabel('Final Compression')
    ax.set_title('Performance Heatmap: Compression (minutes)')
    
    # Add text annotations
    for i in range(len(final_comp_vals)):
        for j in range(len(run_comp_vals)):
            if not np.isnan(matrix[i, j]):
                text = ax.text(j, i, f'{matrix[i, j]:.1f}',
                             ha="center", va="center", color="black", fontsize=10)
    
    plt.colorbar(im, ax=ax, label='Time (minutes)')
    plt.tight_layout()
    
    plot_file = output_dir / f'compression_heatmap_{timestamp}.pdf'
    plt.savefig(plot_file, bbox_inches='tight')
    plt.close()
    print(f"  Created: {plot_file}")
    return plot_file


def create_markdown_tables(results_data):
    """Create markdown tables from results"""
    experiment_type = results_data.get('experiment_type', 'unknown')
    results = [r for r in results_data['results'] if r.get('success', False)]
    
    if not results:
        return "No successful results to display.\n"
    
    # Sort by performance
    results = sorted(results, key=lambda x: x['elapsed_seconds'])
    
    markdown = f"### {experiment_type.replace('_', ' ').title()} Experiment\n\n"
    
    # Summary table
    markdown += "| Rank | Experiment | Time (min) | Stage 1 (s) | Stage 2 (s) | Size (GB) |\n"
    markdown += "|------|------------|------------|-------------|-------------|----------|\n"
    
    for i, r in enumerate(results[:10], 1):  # Top 10
        exp_id = r['experiment_id']
        time_min = r['elapsed_minutes']
        stage1 = r.get('stage_timings', {}).get('stage1', 'N/A')
        stage2 = r.get('stage_timings', {}).get('stage2', 'N/A')
        size = r.get('stats', {}).get('total_size_gb', 'N/A')
        
        stage1_str = f"{stage1:.1f}" if isinstance(stage1, (int, float)) else stage1
        stage2_str = f"{stage2:.1f}" if isinstance(stage2, (int, float)) else stage2
        size_str = f"{size:.2f}" if isinstance(size, (int, float)) else size
        
        markdown += f"| {i} | {exp_id} | {time_min:.2f} | {stage1_str} | {stage2_str} | {size_str} |\n"
    
    markdown += "\n"
    
    # Parameters table for top 5
    markdown += "#### Top 5 Configurations\n\n"
    markdown += "| Rank | Workers | Shards | Chunk Rows | Compression (run/final) | Time (min) |\n"
    markdown += "|------|---------|--------|------------|------------------------|------------|\n"
    
    for i, r in enumerate(results[:5], 1):
        params = r['params']
        workers = params.get('workers', 'N/A')
        shards = params.get('shards', 'N/A')
        chunk_rows = params.get('chunk_rows', 'N/A')
        run_comp = params.get('run_compression', 'N/A')
        final_comp = params.get('final_compression', 'N/A')
        time_min = r['elapsed_minutes']
        
        chunk_str = f"{chunk_rows/1e6:.1f}M" if isinstance(chunk_rows, (int, float)) else chunk_rows
        comp_str = f"{run_comp}/{final_comp}"
        
        markdown += f"| {i} | {workers} | {shards} | {chunk_str} | {comp_str} | {time_min:.2f} |\n"
    
    markdown += "\n"
    
    return markdown


def generate_performance_report(all_results, output_dir):
    """Generate comprehensive performance report"""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    report_file = output_dir / f"PERFORMANCE_REPORT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    with open(report_file, 'w') as f:
        f.write("# MEDS ETL Performance Benchmarking Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("---\n\n")
        
        # Summary section
        f.write("## Executive Summary\n\n")
        
        # Find best overall configuration
        all_successful = []
        for exp_name, data in all_results.items():
            successful = [r for r in data['results'] if r.get('success', False)]
            all_successful.extend([(exp_name, r) for r in successful])
        
        if all_successful:
            best_exp, best_result = min(all_successful, key=lambda x: x[1]['elapsed_seconds'])
            f.write(f"**Best Configuration:** {best_result['experiment_id']} from {best_exp} experiment\n")
            f.write(f"- **Time:** {best_result['elapsed_minutes']:.2f} minutes\n")
            f.write(f"- **Workers:** {best_result['params'].get('workers', 'N/A')}\n")
            f.write(f"- **Shards:** {best_result['params'].get('shards', 'N/A')}\n")
            if 'stage_timings' in best_result:
                f.write(f"- **Stage 1:** {best_result['stage_timings'].get('stage1', 'N/A'):.1f}s\n")
                f.write(f"- **Stage 2:** {best_result['stage_timings'].get('stage2', 'N/A'):.1f}s\n")
            f.write("\n")
        
        f.write("---\n\n")
        
        # Experiment sections
        f.write("## Experiment Results\n\n")
        
        # Workers experiment
        if 'workers' in all_results:
            f.write("### 1. Worker Count Optimization\n\n")
            f.write("Tests the impact of parallel worker count on ETL performance.\n\n")
            f.write(create_markdown_tables(all_results['workers']))
            f.write("---\n\n")
        else:
            f.write("### 1. Worker Count Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment workers`*\n\n")
            f.write("---\n\n")
        
        # Shards experiment
        if 'shards' in all_results:
            f.write("### 2. Shard Count Optimization\n\n")
            f.write("Tests the impact of output shard count on merge performance.\n\n")
            f.write(create_markdown_tables(all_results['shards']))
            f.write("---\n\n")
        else:
            f.write("### 2. Shard Count Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment shards`*\n\n")
            f.write("---\n\n")
        
        # Chunk rows experiment
        if 'chunk_rows' in all_results:
            f.write("### 3. Chunk Size Optimization\n\n")
            f.write("Tests the impact of chunk row count on memory usage and sort performance.\n\n")
            f.write(create_markdown_tables(all_results['chunk_rows']))
            f.write("---\n\n")
        else:
            f.write("### 3. Chunk Size Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment chunk_rows`*\n\n")
            f.write("---\n\n")
        
        # Compression experiment
        if 'compression' in all_results:
            f.write("### 4. Compression Strategy Optimization\n\n")
            f.write("Tests different compression combinations for intermediate runs and final output.\n\n")
            f.write(create_markdown_tables(all_results['compression']))
            f.write("---\n\n")
        else:
            f.write("### 4. Compression Strategy Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment compression`*\n\n")
            f.write("---\n\n")
        
        # Memory experiment
        if 'memory' in all_results:
            f.write("### 5. Memory Configuration Optimization\n\n")
            f.write("Tests different memory configurations including low-memory mode and row group sizes.\n\n")
            f.write(create_markdown_tables(all_results['memory']))
            f.write("---\n\n")
        else:
            f.write("### 5. Memory Configuration Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment memory`*\n\n")
            f.write("---\n\n")
        
        # Workers and shards experiment
        if 'workers_and_shards' in all_results:
            f.write("### 6. Combined Workers × Shards Optimization\n\n")
            f.write("Tests combinations of worker count and shard count for overall optimization.\n\n")
            f.write(create_markdown_tables(all_results['workers_and_shards']))
            f.write("---\n\n")
        else:
            f.write("### 6. Combined Workers × Shards Optimization\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment workers_and_shards`*\n\n")
            f.write("---\n\n")
        
        # Full experiment
        if 'full' in all_results:
            f.write("### 7. Full Grid Search\n\n")
            f.write("Comprehensive grid search across multiple parameters simultaneously.\n\n")
            f.write(create_markdown_tables(all_results['full']))
            f.write("---\n\n")
        else:
            f.write("### 7. Full Grid Search\n\n")
            f.write("*Experiment not yet run. Run with: `--experiment full`*\n\n")
            f.write("---\n\n")
        
        # Recommendations
        f.write("## Recommendations\n\n")
        if all_successful:
            best_exp, best_result = min(all_successful, key=lambda x: x[1]['elapsed_seconds'])
            params = best_result['params']
            
            f.write("Based on benchmark results, the recommended configuration is:\n\n")
            f.write("```bash\n")
            f.write("python -m meds_etl.omop_refactor_streaming \\\n")
            f.write("  --omop_dir INPUT_DIR \\\n")
            f.write("  --output_dir OUTPUT_DIR \\\n")
            f.write("  --config CONFIG.json \\\n")
            f.write("  --force-refresh \\\n")
            f.write(f"  --workers {params.get('workers', 8)} \\\n")
            f.write(f"  --shards {params.get('shards', 10)} \\\n")
            f.write(f"  --chunk_rows {params.get('chunk_rows', 10000000)} \\\n")
            f.write(f"  --run_compression {params.get('run_compression', 'lz4')} \\\n")
            f.write(f"  --final_compression {params.get('final_compression', 'zstd')}\n")
            if params.get('low_memory'):
                f.write("  --low_memory \\\n")
            f.write("```\n\n")
        else:
            f.write("Run benchmarking experiments to generate recommendations.\n\n")
        
        f.write("---\n\n")
        f.write("*Note: Performance may vary based on dataset size, system hardware, and available memory.*\n")
    
    print(f"\n✓ Performance report generated: {report_file}")
    return report_file


def main():
    parser = argparse.ArgumentParser(
        description="Analyze benchmark results and generate reports"
    )
    
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="Input JSON files (can use wildcards)"
    )
    parser.add_argument(
        "--output_dir",
        default="reports",
        help="Output directory for reports and plots (default: reports/)"
    )
    parser.add_argument(
        "--format",
        choices=["plots", "markdown", "both"],
        default="both",
        help="Output format (default: both)"
    )
    
    args = parser.parse_args()
    
    # Collect all results
    all_results = {}
    
    print("Loading results...")
    for pattern in args.input:
        from glob import glob
        for json_file in glob(pattern):
            print(f"  Loading: {json_file}")
            data = load_results(json_file)
            exp_type = data.get('experiment_type', 'unknown')
            all_results[exp_type] = data
    
    if not all_results:
        print("No results found!")
        return 1
    
    print(f"\nLoaded {len(all_results)} experiment(s)")
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate plots
    if args.format in ["plots", "both"]:
        print("\nGenerating plots...")
        for exp_type, data in all_results.items():
            print(f"\n{exp_type}:")
            create_performance_plots(data, output_dir)
    
    # Generate performance report
    if args.format in ["markdown", "both"]:
        print("\nGenerating performance report...")
        generate_performance_report(all_results, output_dir)
    
    print(f"\n✓ Analysis complete! Check {output_dir}/ for outputs")
    return 0


if __name__ == "__main__":
    sys.exit(main())

