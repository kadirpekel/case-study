import re
import matplotlib.pyplot as plt
import os
import glob

def parse_log(filepath):
    data = []
    start_ts = None
    if not os.path.exists(filepath): return []
    with open(filepath, 'r') as f:
        for line in f:
            match = re.search(r'\[(\d+)\] .* Solved: (\d+)', line)
            if match:
                ts, solved = int(match.group(1)), int(match.group(2))
                if start_ts is None: start_ts = ts
                data.append((ts - start_ts, solved))
    return data

def calculate_rolling_throughput(data, window_size=10):
    if not data: return [], []
    start_time, end_time = data[0][0], data[-1][0]
    throughput_vals, time_points = [], []
    current_t = start_time
    while current_t <= end_time:
        s_count = 0
        for t, s in data:
            if t <= current_t: s_count = s
            else: break
        e_count = data[-1][1]
        for t, s in data:
            if t <= current_t + window_size: e_count = s
            else: break
        throughput_vals.append((e_count - s_count) / window_size)
        time_points.append(current_t)
        current_t += 1
    return time_points, throughput_vals

def calculate_sustained_metric(data):
    if not data or len(data) < 5: return 0.0
    total_duration = data[-1][0]
    if total_duration <= 0: return 0.0
    s_cutoff, e_cutoff = total_duration * 0.1, total_duration * 0.9
    s_solved, e_solved = 0, data[-1][1]
    for t, s in data:
        if t <= s_cutoff: s_solved = s
        if t <= e_cutoff: e_solved = s
    return (e_solved - s_solved) / (e_cutoff - s_cutoff) if (e_cutoff - s_cutoff) > 0 else 0

def main():
    log_dir = 'comparison'
    files = glob.glob(os.path.join(log_dir, "*.txt"))
    results = {}
    for f in files:
        name = os.path.basename(f).replace(".txt", "")
        if name == "current_w20": label = "Current (w20 batch)"
        elif name == "proposed_w8_p25": label = "Proposed (w8-p25)"
        elif name == "proposed_w8_p50": label = "Proposed (w8-p50)"
        else:
            parts = name.split("_")
            # Extract pXX (where XX are digits)
            p_match = [p for p in parts if re.match(r'^p\d+$', p)]
            p_val = p_match[0] if p_match else "p?"
            rpm_match = [p for p in parts if p.startswith("rpm")]
            rpm_val = rpm_match[0].replace("rpm", "") if rpm_match else "Max"
            label = f"Proposed (w8-{p_val}, {rpm_val} RPM)"
        
        data = parse_log(f)
        if data:
            results[label] = {'data': data, 'stats': {
                'total_time': data[-1][0],
                'avg_tps': data[-1][1] / data[-1][0] if data[-1][0] > 0 else 0,
                'sustained_tps': calculate_sustained_metric(data)
            }}

    sorted_labels = sorted(results.keys(), key=lambda x: (0 if 'Current' in x else 1, x))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 14))
    colors = plt.cm.tab10.colors
    
    print("\n" + "="*90)
    print(f"{'Configuration':<45} | {'Duration':>10} | {'Avg TPS':>8} | {'Steady TPS':>12}")
    print("-" * 90)
    for i, label in enumerate(sorted_labels):
        res = results[label]
        s = res['stats']
        c = colors[i % len(colors)]
        ax1.plot([d[0] for d in res['data']], [d[1] for d in res['data']], label=label, lw=2, color=c)
        tx, ty = calculate_rolling_throughput(res['data'], 10)
        ax2.plot(tx, ty, label=label, lw=2, color=c)
        print(f"{label:<45} | {s['total_time']:>9}s | {s['avg_tps']:>8.2f} | {s['sustained_tps']:>11.2f}")
    
    ax1.set_title('Progress (Solved Tasks)'); ax1.set_xlabel('Time (s)'); ax1.set_ylabel('Solved'); ax1.grid(True); ax1.legend()
    ax2.set_title('Throughput Stability (10s rolling)'); ax2.set_xlabel('Time (s)'); ax2.set_ylabel('TPS'); ax2.grid(True); ax2.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(log_dir, 'performance_comparison_detailed.png'))
    print("="*90)

if __name__ == "__main__":
    main()
