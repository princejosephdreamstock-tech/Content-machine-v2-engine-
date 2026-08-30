import csv
import random
from engine_common import get_channel_config

class MissingChannelConfigError(Exception):
    pass

_CACHE = {}

def load_atp_queries(channel_id):
    """Load ATP queries once per channel, cache in memory"""
    if channel_id in _CACHE:
        return _CACHE[channel_id]

    config = get_channel_config(channel_id)
    filepath = config.get("topic_csv_path")
    if not filepath:
        raise MissingChannelConfigError(
            f"channel '{channel_id}' has no 'topic_csv_path' set in config_json — refusing to fall back to a generic default"
        )

    queries = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            query = row.get('Zoekwoord', '').strip()
            if query:
                queries.append(query)

    if not queries:
        raise MissingChannelConfigError(
            f"channel '{channel_id}' topic_csv_path '{filepath}' loaded zero queries — refusing to fall back to a generic topic"
        )

    _CACHE[channel_id] = queries
    return queries

def get_random_topic(channel_id):
    """Get one random ATP query — raises loudly if config or CSV is missing/empty"""
    queries = load_atp_queries(channel_id)
    return random.choice(queries)
