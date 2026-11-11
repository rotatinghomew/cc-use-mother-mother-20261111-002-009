from __future__ import annotations
import argparse
import json
from trial_site_federator.service import run_fixture

def main() -> int:
    parser=argparse.ArgumentParser(description='肿瘤临床试验站点采集联邦器')
    parser.add_argument("fixture")
    parser.add_argument("--db",default="/tmp/trial_site_federator.sqlite3")
    args=parser.parse_args()
    result=run_fixture(args.fixture,args.db)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))
    return 0

if __name__ == "__main__": raise SystemExit(main())
