#!/usr/bin/env python

import datetime
import pathlib
import ssl
from itertools import islice

import feedparser  # Parses RSS feeds
import listparser  # Parses OPML files
import requests  # To make downloads easier
from tenacity import retry, stop_after_attempt
from tinydb import Query, TinyDB
from tinydb.storages import JSONStorage
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization.serializers import DateTimeSerializer
from tqdm import tqdm


@retry(stop=stop_after_attempt(3))
def parse_feed(url: str):
    """
    Uses feedparser to download the RSS feed XML file
    """
    # Disable SSL verification otherwise feedparser won't work
    if hasattr(ssl, "_create_unverified_context"):
        ssl._create_default_https_context = ssl._create_unverified_context
    print(f"Getting feed for {url}")
    parsed_feed = feedparser.parse(url)
    return parsed_feed


@retry(stop=stop_after_attempt(3))
def download_file(url, dest: pathlib.Path = None):
    # Disable SSL warnings
    requests.packages.urllib3.disable_warnings()
    response = requests.get(url, stream=True, verify=False)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 8092
    title = dest.stem
    progress_bar = tqdm(
        total=total_size_in_bytes,
        unit="bytes",
        unit_scale=True,
        unit_divisor=block_size,
        ncols=len(title) + 75,
        desc=title,
    )
    try:
        response.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8092):
                progress_bar.update(len(chunk))
                f.write(chunk)
    except requests.exceptions.HTTPError as e:
        print(f"There was an issue downloading {url}. Error: {e}")


def download_episode(entry, dir: pathlib.Path):
    """
    Downloads individual episode using chunking via requests
    """
    download_path = dir / f"{entry['title']}.mp3"
    if download_path.exists():
        print(f"{entry['title']} already downloaded.")
        return
    for link in entry["links"]:
        if link["type"] in ["audio/mpeg"]:
            print(f'Downloading {entry["title"]}')
            download_file(link["href"], download_path)


def download_feed(feed, limit: int = 3):
    """
    Initiates downloads of a single feed (a show in Podcast parlance).
    This function merely coordinates calls to sub-functions which handle the actual downloading.
    """
    cwd = pathlib.Path.cwd()
    # feed_dir = cwd / ''.join(x if x.isalnum() else "_" for x in feed["title"])
    feed_dir = cwd / feed["title"].replace("/", "_")
    if not feed_dir.exists():
        feed_dir.mkdir(parents=True)
    for entry in islice(feed["entries"], 0, limit):
        download_episode(entry, feed_dir)


def older_than_1_day(dt):
    return dt is None or (datetime.datetime.utcnow() - dt).days > 1


def update_feeds(feeds, max_entries_per_feed=5, force_updates=False):
    """
    Initiate downloading and parsing all feeds
    """
    Feed = Query()
    if force_updates:
        unfresh_feeds = feeds.all()
    else:
        unfresh_feeds = feeds.search(Feed.last_updated.test(older_than_1_day))
    parsed_feeds = []
    for feed in unfresh_feeds:
        entries = parse_feed(feed["url"])["entries"][:max_entries_per_feed]
        feeds.update({"entries": entries, "last_updated": datetime.datetime.utcnow()}, Feed.title == feed["title"])
    return parsed_feeds


def parse_opml(file):
    opml_feeds = [
        {"title": feed["title"], "url": feed["url"], "entries": [], "last_updated": None}
        for feed in listparser.parse(file)["feeds"]
    ]
    return opml_feeds


def main(args):
    serialization = SerializationMiddleware(JSONStorage)
    serialization.register_serializer(DateTimeSerializer(), "TinyDate")
    db = TinyDB("feeds.json", sort_keys=True, indent=4, separators=(",", ": "), storage=serialization)
    feeds = db.table("feeds")
    if args.import_opml:
        opml_feeds = parse_opml("podcasts_opml.xml")
        for feed in opml_feeds:
            feeds.upsert(feed, Query()["title"] == feed["title"])

    # if meta_table.contains(doc_id=1):
    #     last_updated = meta_table.get(doc_id=0)["last_updated"]
    # else:
    #     meta_table.insert(Document({"last_updated": now}, doc_id=1))
    #     last_updated = now
    # if (now - last_updated).days > 1:
    update_feeds(feeds, force_updates=args.force_updates)
    for feed in feeds.all():
        download_feed(feed, args.max_episodes)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--import-opml",
        help="Import a new list of feeds with an OPML file, exported from another service.",
    )
    parser.add_argument(
        "-f",
        "--force-updates",
        action="store_true",
        help='Force updates to feeds, even if they"re not stale',
    )
    parser.add_argument("-m", "--max-episodes", type=int, help="Max number of episodes to keep for a show")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=lambda p: pathlib.Path(p).absolute(),
        default=pathlib.Path(__file__).absolute().parent / "podcasts",
        help="Location to download files to",
    )
    args = parser.parse_args()
    main(args)
