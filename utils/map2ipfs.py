import ipfsapi
import sys
import json
import requests
import os
import logging

api = ipfsapi.connect('127.0.0.1', 5001)

def concat(*paths: list) -> str:
    return "/".join(paths)

def is_url(s: str) -> bool:
    return isinstance(s, str) and s.startswith("http")

def fetch_and_save(url: str, retry=5) -> str:
    if retry == 0:
        raise Exception("Failed in %s times" % retry)
    print("mapping %s" % url)
    path, filename = url.split("pokemontcg.io/")[1].split('/')

    if os.path.exists(concat(path, filename)):
        return concat(path, filename)

    if not os.path.exists(path):
        os.mkdir(path)
    resp = requests.get(
        url,
        headers={
            'User-Agent': "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
        },
        stream=True,
        allow_redirects=False
    )
    # if resp.status_code != 200:
    #     return fetch_and_save(url, retry-1)

    with open(concat(path, filename), "wb") as f:
        f.write(resp.raw.read())
        return concat(path, filename)

def mapper(data):
    if isinstance(data, list):
        return list(map(mapper, data))
    if is_url(data):
        filename = fetch_and_save(data)
        ipfsuri = api.add(filename)
        return "ipfs/{hash}".format(hash=ipfsuri['Hash'])
    if isinstance(data, dict):
        return {k: mapper(v) for k, v in data.items()}
    return data

def main():
    path = sys.argv[-2]
    target = sys.argv[-1]
    dataset = os.listdir(path)
    for d in dataset:
        if d.endswith(".json"):
            with open(concat(path, d), "rb") as f:
                origin = json.loads(f.read())
                with open(concat(target, d), "w") as t:
                    t.write(json.dumps(mapper(origin)))



if __name__ == "__main__":
    main()
