import time
import requests
import os

API_URL = "http://localhost:8000"

endpoints = [
    {"name": "Campaign Performance", "path": "/campaign/1/performance"},
    {"name": "Advertiser Spending", "path": "/advertiser/1/spending"},
    {"name": "User Engagements", "path": "/user/1/engagements"},
]

def run_benchmark():
    results = []
    
    print("Running benchmarks...")
    # Give the API a moment to be fully ready if we just started it
    time.sleep(2)
    
    for ep in endpoints:
        url = f"{API_URL}{ep['path']}"
        print(f"Benchmarking {ep['name']}...")
        
        # 1. Measure without cache (Direct DB Query)
        # We might want to do this a few times and average, but once is fine for demonstration
        start = time.time()
        resp = requests.get(url, params={"use_cache": "false"})
        no_cache_time = time.time() - start
        
        if resp.status_code != 200:
            print(f"Warning: Endpoint returned {resp.status_code} - {resp.text}")
        
        # 2. Measure with cache (First call populates cache)
        requests.get(url, params={"use_cache": "true"})
        
        # 3. Measure with cache (Second call hits cache)
        start = time.time()
        resp = requests.get(url, params={"use_cache": "true"})
        cache_time = time.time() - start
        
        results.append({
            "Endpoint": ep["name"],
            "Without Cache (s)": f"{no_cache_time:.4f}",
            "With Cache (s)": f"{cache_time:.4f}",
            "Speedup": f"{no_cache_time / cache_time:.2f}x" if cache_time > 0 else "N/A"
        })

    print("\nBenchmarking Results:")
    print("| Endpoint | Without Cache (s) | With Cache (s) | Speedup |")
    print("|----------|-------------------|----------------|---------|")
    for r in results:
        print(f"| {r['Endpoint']} | {r['Without Cache (s)']} | {r['With Cache (s)']} | {r['Speedup']} |")
        
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "benchmark_results.md")
    with open(output_path, "w") as f:
        f.write("# Benchmarking Results\n\n")
        f.write("| Endpoint | Without Cache (s) | With Cache (s) | Speedup |\n")
        f.write("|----------|-------------------|----------------|---------|\n")
        for r in results:
            f.write(f"| {r['Endpoint']} | {r['Without Cache (s)']} | {r['With Cache (s)']} | {r['Speedup']} |\n")
    print(f"\nBenchmark results saved to {output_path}")

if __name__ == "__main__":
    run_benchmark()
