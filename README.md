# concurrent-client
Concurrent requests that respect 'RateLimit-Limit' and 'RateLimit-Reset' headers.

```
python -m venv venv
source venv/bin/activate
pip install aiohttp
```

Test run:
```
API_KEY=<secrets> python concurrent_client.py 10 1000 https://some.api.app/
```
