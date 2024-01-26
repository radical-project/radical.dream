import pandas as pd
import matplotlib.pyplot as plt

# Assuming your data is in a CSV file named 'your_data.csv'
# Replace 'your_data.csv' with the actual file name or provide the DataFrame directly

# Read the data into a DataFrame
data = pd.read_csv('resource_usage_files/stress_ng_hydraa_nodes.csv')

# Convert timestamp to datetime format
data['TimeStamp'] = pd.to_datetime(data['TimeStamp'], unit='s')

# Normalize CPU usage to cores
data['CPUsUsage(N)'] = data['CPUsUsage(N)'].str.rstrip('n').astype(float) / 1e9

# Plot subplots for each node with dual y-axes
nodes = data['NodeName'].unique()

fig, axes = plt.subplots(len(nodes), 1, figsize=(12, 6 * len(nodes)), sharex=True)

for i, node in enumerate(nodes):
    node_data = data[data['NodeName'] == node]

    # Create left y-axis for CPU
    ax1 = axes[i]

    # Plot CPU usage and capacity
    ax1.plot(node_data['TimeStamp'], node_data['CPUsUsage(N)'], label='CPU Usage (cores)', color='blue')
    ax1.plot(node_data['TimeStamp'], node_data['CPUsCapacity'], label='CPU Capacity (cores)', linestyle='dashed', color='green')
    ax1.set_ylabel('CPU', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')

    # Create right y-axis for Memory
    ax2 = ax1.twinx()

    # Plot Memory usage and capacity
    ax2.plot(node_data['TimeStamp'], node_data['MemoryUsage(Ki)'], label='Memory Usage (KiB)', color='orange')
    ax2.plot(node_data['TimeStamp'], node_data['MemoryCapacity(Ki)'], label='Memory Capacity (KiB)', linestyle='dashed', color='red')
    ax2.set_ylabel('Memory', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')

    axes[i].set_title(f'Node: {node}')
    axes[i].set_xlabel('Timestamp')
    axes[i].grid(True)

    # Add legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper left')

# Adjust layout
plt.tight_layout()

# Show the plot
plt.savefig('nodes_resources.jpg')