import asyncio
import http.client
import json
import os
import sys
import time

from asyncio import Semaphore

from aiohttp import ClientSession, TCPConnector


RETRY_SLEEP_LOCK = asyncio.Lock()
RETRY_COUNTER = 0


async def async_http_request(session: ClientSession, method: str, headers: dict, url: str, payload: str, semaphore: Semaphore):
    async with semaphore:
        async with session.request(method, url, headers=headers, data=payload) as response:
            status = response.status
            content = await response.text()

            response_headers = dict(response.headers)

            retry_after = int(response_headers.get('Retry-After', 0))
            wait_until_ts = int(time.time() + retry_after)

            ratelimit_limit = int(response_headers.get('RateLimit-Limit', sys.maxsize))
            ratelimit_reset = int(response_headers.get('RateLimit-Reset', sys.maxsize))

            """
            TODO: based on what current semaphore count, ratelimit_limit and ratelimit_limit are,
                  should we be slowing down or reducing the semaphore/concurrency amount?
            """

    return status, content, response_headers, wait_until_ts


async def request_with_retry(session: ClientSession, method: str, headers: dict, url: str, payload: str, semaphore: Semaphore):
    try:
        status, content, response_headers, wait_until_ts = await async_http_request(session, method, headers, url, payload, semaphore)

        if status != http.client.TOO_MANY_REQUESTS:
            return url, status, content, ''

        async with RETRY_SLEEP_LOCK:
            global RETRY_COUNTER
            RETRY_COUNTER += 1

            sleep_seconds = max(wait_until_ts - time.time(), 0)

            print(f"[request_with_retry] TOO_MANY_REQUESTS status: {status}, sleep_seconds: {sleep_seconds}")
            print(f"[request_with_retry] response_headers: {response_headers}")

            await asyncio.sleep(sleep_seconds)

        return await request_with_retry(session, method, headers, url, payload, semaphore)
    except Exception as e:
        return url, None, None, e


async def batch_requests(method, headers, url, payloads, concurrency):
    requests = [(method.upper(), headers, url, payload) for payload in payloads]

    semaphore = Semaphore(concurrency)
    async with ClientSession(connector=TCPConnector(limit=concurrency)) as session:
        tasks = [request_with_retry(session, method, headers, url, payload, semaphore) for method, headers, url, payload in requests]

        counter = 0
        for task in asyncio.as_completed(tasks):
            counter += 1
            yield await task

            if not counter % 369:
                print(f"[counter] {counter}")


async def main(requests, concurrency, api):
    headers = {'Content-Type': 'application/json', 'Authorization': f"Bearer {os.environ['METERED_API_KEY']}"}
    payload = json.dumps({'query': '{ ping }'})

    payloads = [payload for _ in range(requests)]

    start_time = time.time()
    results = []
    async for result in batch_requests('POST', headers, api, payloads, concurrency):
        results.append(result)
        print(result)

    print(f"Total Time: {time.time() - start_time}, RETRY_COUNTER: {RETRY_COUNTER} len(results): {len(results)}")


if __name__ == '__main__':
    requests = int(sys.argv[1])
    concurrency = int(sys.argv[2])
    api = sys.argv[3]

    print(f"[__main__] requests: {requests}, concurrency: {concurrency}, api: '{api}'")
    asyncio.run(main(requests=requests, concurrency=concurrency, api=api))
