import asyncio
import json
import os
import sys
import time

from asyncio import Semaphore

from aiohttp import ClientSession, TCPConnector


RETRY_SLEEP_LOCK = asyncio.Lock()
RETRY_COUNTER = 0
TOO_MANY_REQUESTS = 429


class RemainingRequests:
    def __init__(self, value):
        self.value = value
        self._lock = asyncio.Lock()

    async def decrement(self):
        async with self._lock:
            self.value -= 1
            return self.value

    async def get(self):
        async with self._lock:
            return self.value


async def async_http_request(
    session: ClientSession, method: str, headers: dict, url: str, payload: str, semaphore: Semaphore, remaining_requests: RemainingRequests
):
    async with semaphore:
        async with session.request(method, url, headers=headers, data=payload) as response:
            status = response.status
            content = await response.text()

            response_headers = dict(response.headers)

            limit = int(response_headers.get('Ratelimit-Limit', sys.maxsize))
            limit_remaining = int(response_headers.get('Ratelimit-Remaining', sys.maxsize))
            limit_reset = int(response_headers.get('Ratelimit-Reset', sys.maxsize))

            wait_until_ts = 0
            if status == TOO_MANY_REQUESTS:
                """
                TODO: We are stuck estimating a proper Retry-After amount on our own,
                since chattiness remains once we use up limit_remaining.

                Even if we safely reduce the concurrency semaphore to 1, only allowing
                one request at a time, the retries sleep whatever few seconds they are asked
                to and then retry again and again.

                Or, there is a subtle bug!
                """
                retry_after = int(response_headers.get('Retry-After', 0))
                wait_until_ts = int(time.time() + retry_after)

                remaining = await remaining_requests.get()
            else:
                remaining = await remaining_requests.decrement()

            print(
                f"[async_http_request] limit: {limit}, remaining: {limit_remaining} reset: {limit_reset}, remaining_requests: {remaining_requests}"
            )

            if remaining > limit_remaining:
                while semaphore._value > limit_remaining + 10:
                    print(f"[async_http_request] reducing concurrency: {semaphore._value}")
                    await semaphore.acquire()
                    await asyncio.sleep(0.01)
            elif remaining < limit_remaining:
                while semaphore._value < remaining:
                    print(f"[async_http_request] increasing concurrency: {semaphore._value}")
                    await semaphore.release()
                    await asyncio.sleep(0.01)

    return status, content, response_headers, wait_until_ts


async def request_with_retry(
    session: ClientSession, method: str, headers: dict, url: str, payload: str, semaphore: Semaphore, remaining_requests: RemainingRequests
):
    try:
        status, content, response_headers, wait_until_ts = await async_http_request(
            session, method, headers, url, payload, semaphore, remaining_requests
        )

        if status != TOO_MANY_REQUESTS:
            return url, status, content, ''

        async with RETRY_SLEEP_LOCK:
            global RETRY_COUNTER
            RETRY_COUNTER += 1

            sleep_seconds = max(wait_until_ts - time.time(), 0)

            print(f"[request_with_retry] TOO_MANY_REQUESTS status: {status}, sleep_seconds: {sleep_seconds}")
            print(f"[request_with_retry] response_headers: {response_headers}")

            await asyncio.sleep(sleep_seconds)

        return await request_with_retry(session, method, headers, url, payload, semaphore, remaining_requests)
    except Exception as e:
        return url, None, None, e


async def batch_requests(method, headers, url, payloads, concurrency):
    requests = [(method.upper(), headers, url, payload) for payload in payloads]

    remaining_requests = RemainingRequests(len(requests))
    semaphore = Semaphore(concurrency)
    async with ClientSession(connector=TCPConnector(limit=concurrency)) as session:
        tasks = [
            request_with_retry(session, method, headers, url, payload, semaphore, remaining_requests)
            for method, headers, url, payload in requests
        ]

        counter = 0
        for task in asyncio.as_completed(tasks):
            counter += 1
            yield await task

            if not counter % 369:
                print(f"[counter] {counter}")


async def main(requests, concurrency, api):
    headers = {'Content-Type': 'application/json', 'Authorization': f"Bearer {os.environ['API_KEY']}"}
    payload = json.dumps({'query': '{ ping }'})

    payloads = [payload for _ in range(requests)]

    start_time = time.time()
    results = [result async for result in batch_requests('POST', headers, api, payloads, concurrency)]

    print(f"Total Time: {time.time() - start_time}, RETRY_COUNTER: {RETRY_COUNTER} len(results): {len(results)}")
    for result in results:
        print(result)


if __name__ == '__main__':
    requests = int(sys.argv[1])
    concurrency = int(sys.argv[2])
    api = sys.argv[3]

    print(f"[__main__] requests: {requests}, concurrency: {concurrency}, api: '{api}'")
    asyncio.run(main(requests=requests, concurrency=concurrency, api=api))
