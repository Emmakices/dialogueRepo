import os
import urllib.request

healthcheck_url = f"{os.environ['API_BASEURL']}/health"
print(f"GET {healthcheck_url}:")

with urllib.request.urlopen(healthcheck_url) as response:
    print(response.read().decode())