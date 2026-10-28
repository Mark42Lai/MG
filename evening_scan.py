"""Recover due Taiwan evening batches with durable per-day receipts."""
import base64
import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

TAIPEI = timezone(timedelta(hours=8))
OFFSETS = tuple(range(0, 2281, 380))


def due_offsets(now):
    if now.weekday() >= 5 or (now.hour, now.minute) < (17, 15):
        return []
    return [offset for offset in OFFSETS
            if (now.hour, now.minute) >= (17 + offset // 380, 15)]


class Receipts:
    def __init__(self):
        repo = os.environ['GITHUB_REPOSITORY']
        self.url = f'https://api.github.com/repos/{repo}/contents/scan-receipts.json'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
            'Accept': 'application/vnd.github+json',
        })

    def read(self):
        response = self.session.get(self.url, params={'ref': 'ops/scan-state'}, timeout=30)
        if response.status_code == 404:
            return {}, None
        response.raise_for_status()
        body = response.json()
        return json.loads(base64.b64decode(body['content'])), body['sha']

    def save(self, state, sha):
        payload = {
            'branch': 'ops/scan-state',
            'message': 'Record completed Taiwan scan batch',
            'content': base64.b64encode(json.dumps(state, indent=2).encode()).decode(),
        }
        if sha:
            payload['sha'] = sha
        response = self.session.put(self.url, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()['content']['sha']


def main():
    now = datetime.now(TAIPEI)
    due = due_offsets(now)
    if not due:
        print(f'{now.isoformat()}: outside Taiwan weekday evening; no scan', flush=True)
        return
    # Freeze the date at entry, including batches waiting for quota past midnight.
    trade_date = now.date().isoformat()
    receipts = Receipts()
    state, sha = receipts.read()
    completed = set(state.get(trade_date, []))
    while True:
        clock = datetime.now(TAIPEI)
        if clock.date() != now.date():
            clock = now.replace(hour=23, minute=59)
        pending = [offset for offset in due_offsets(clock) if offset not in completed]
        if not pending:
            print(f'{trade_date}: all currently due batches complete', flush=True)
            break
        offset = pending[0]
        print(f'{trade_date} offset={offset}: starting due batch', flush=True)
        env = dict(os.environ, SCAN_TRADE_DATE=trade_date,
                   FINAL_BATCH='1' if offset == 2280 else '0')
        subprocess.run([sys.executable, '-u', str(Path(__file__).with_name('gsr_scanner.py')),
                        '--offset', str(offset), '--limit', '380'], env=env, check=True)
        completed.add(offset)
        state[trade_date] = sorted(completed)
        # Retain recent receipts so yesterday's delayed jobs cannot erase history.
        state = dict(sorted(state.items())[-14:])
        sha = receipts.save(state, sha)
        print(f'{trade_date} offset={offset}: completion saved', flush=True)


if __name__ == '__main__':
    main()
