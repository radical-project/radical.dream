
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('hydraa.sandbox/provider/pods_resources.csv')

# Convert CPUsUsage and MemoryUsage columns to numeric (remove 'n' and 'Ki' and convert to appropriate units)
df['CPUsUsage(M)'] = pd.to_numeric(df['CPUsUsage(M)'].str.rstrip('n'), errors='coerce') / 1e6  # Convert nanoseconds to milliseconds
df['MemoryUsage(Ki)'] = pd.to_numeric(df['MemoryUsage(Ki)'].str.rstrip('Ki'), errors='coerce') / 1024  # Convert KiB to MB

# Normalize CPUsUsage to percentage
df['CPUsUsage(%)'] = (df['CPUsUsage(M)'] / df['CPUsUsage(M)'].max()) * 100

# Plotting
plt.figure(figsize=(10, 6))

for pod in df['PodName'].unique():
    pod_data = df[df['PodName'] == pod]
    plt.plot(pod_data['TimeStamp'], pod_data['CPUsUsage(%)'], label=f'{pod} CPUsUsage(%)')
    plt.plot(pod_data['TimeStamp'], pod_data['MemoryUsage(Ki)'], label=f'{pod} MemoryUsage(MB)')

plt.xlabel('TimeStamp')
plt.ylabel('Usage')
plt.title('Normalized CPUsUsage and MemoryUsage over Time')
plt.legend()
plt.savefig('pods_resources.png')
