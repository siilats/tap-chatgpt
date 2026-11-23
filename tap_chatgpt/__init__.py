"""Read account inventory from the OpenAI Advertiser API as Singer records."""

import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

API_URL = "https://api.ads.openai.com/v1"
STREAMS = ("campaigns", "ad_groups", "ads")
PARENT_KEYS = {"campaigns": (), "ad_groups": ("campaign_id",),
               "ads": ("campaign_id", "ad_group_id")}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class AdvertiserAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.opener = build_opener(NoRedirect())

    def get(self, stream, params):
        request = Request(
            f"{API_URL}/{stream}?{urlencode(params)}",
            headers={"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"},
        )
        for attempt in range(5):
            delay = 2 ** attempt
            try:
                with self.opener.open(request, timeout=60) as response:
                    return json.load(response)
            except HTTPError as error:
                status = error.code
                retry_after = error.headers.get("Retry-After", "")
                error.close()
                if attempt == 4 or (status != 429 and status not in (500, 502, 503, 504)):
                    raise RuntimeError(f"OpenAI {stream} request failed (HTTP {status})") from None
                if retry_after.isdecimal():
                    delay = max(delay, int(retry_after))
            except (URLError, TimeoutError):
                if attempt == 4:
                    raise RuntimeError(f"OpenAI {stream} request failed after retries") from None
            time.sleep(delay)

    def records(self, stream, **filters):
        params = {"limit": 100, "order": "asc", **filters}
        seen_cursors = set()
        while True:
            page = self.get(stream, params)
            if (not isinstance(page, dict) or not isinstance(page.get("data"), list)
                    or not isinstance(page.get("has_more"), bool)):
                raise ValueError(f"Invalid {stream} page")
            for record in page["data"]:
                if not isinstance(record, dict) or not isinstance(record.get("id"), str) or not record["id"]:
                    raise ValueError(f"Invalid {stream} record ID")
                yield record
            if not page["has_more"]:
                return
            cursor = page.get("last_id")
            if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
                raise ValueError(f"Invalid or repeated {stream} pagination cursor")
            seen_cursors.add(cursor)
            params["after"] = cursor


def discover():
    streams = []
    for stream in STREAMS:
        properties = {
            "id": {"type": "string"},
            **{key: {"type": "string"} for key in PARENT_KEYS[stream]},
            "name": {"type": ["null", "string"]},
            "status": {"type": ["null", "string"]},
            "created_at": {"type": ["null", "integer"]},
            "updated_at": {"type": ["null", "integer"]},
            "payload": {"type": "object", "additionalProperties": True},
        }
        streams.append({
            "stream": stream,
            "tap_stream_id": stream,
            "key_properties": ["id"],
            "schema": {"type": "object", "properties": properties},
            "metadata": [
                {"breadcrumb": [], "metadata": {
                    "selected-by-default": True,
                    "table-key-properties": ["id"],
                    "forced-replication-method": "FULL_TABLE",
                }},
                *[
                    {"breadcrumb": ["properties", key], "metadata": {
                        "inclusion": "automatic" if key == "id" else "available",
                        "selected-by-default": True,
                    }}
                    for key in properties
                ],
            ],
        })
    return {"streams": streams}


def emit(message_type, **fields):
    print(json.dumps({"type": message_type, **fields}, ensure_ascii=True))


def selected_streams(catalog):
    selected = {}
    for entry in catalog["streams"]:
        metadata = {tuple(item["breadcrumb"]): item["metadata"] for item in entry["metadata"]}
        root = metadata.get((), {})
        if root.get("selected", root.get("selected-by-default", True)):
            stream = entry["tap_stream_id"]
            properties = {
                key: schema for key, schema in entry["schema"]["properties"].items()
                if key == "id" or metadata.get(("properties", key), {}).get("selected", True)
            }
            selected[stream] = {**entry["schema"], "properties": properties}
    return selected


def sync(api, selected):
    def write(stream, source, **parents):
        if stream in selected:
            record = {**source, **parents, "payload": source}
            emit("RECORD", stream=stream, record={
                key: record[key] for key in selected[stream]["properties"] if key in record
            })

    for stream, schema in selected.items():
        emit("SCHEMA", stream=stream, schema=schema, key_properties=["id"])
    if not selected:
        return
    for campaign in api.records("campaigns"):
        write("campaigns", campaign)
        if "ad_groups" not in selected and "ads" not in selected:
            continue
        campaign_id = campaign["id"]
        for group in api.records("ad_groups", campaign_id=campaign_id):
            write("ad_groups", group, campaign_id=campaign_id)
            if "ads" in selected:
                group_id = group["id"]
                for ad in api.records("ads", ad_group_id=group_id):
                    write("ads", ad, campaign_id=campaign_id, ad_group_id=group_id)


def read_json(path):
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config")
    parser.add_argument("--catalog")
    parser.add_argument("--discover", action="store_true")
    args = parser.parse_args()
    if args.discover:
        print(json.dumps(discover()))
        return
    try:
        config = read_json(args.config) if args.config else {}
        api_key = config.get("api_key") or os.environ.get("OPENAI_ADS_API_KEY")
        if not isinstance(api_key, str) or not api_key.strip():
            parser.error("Set OPENAI_ADS_API_KEY or supply api_key in --config")
        catalog = read_json(args.catalog) if args.catalog else discover()
        sync(AdvertiserAPI(api_key.strip()), selected_streams(catalog))
    except (OSError, ValueError, RuntimeError) as error:
        print(f"tap-chatgpt: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
