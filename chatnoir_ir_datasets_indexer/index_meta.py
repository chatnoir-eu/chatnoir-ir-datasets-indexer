MAPPINGS_META = {
    "properties": {
        "uuid": {
            "type": "keyword"
        },
        "source_file": {
            "type": "keyword"
        },
        "source_offset": {
            "type": "long"
        },
        "content_length": {
            "type": "long"
        },
        "content_type": {
            "type": "keyword"
        },
        "warc_date": {
            "type": "date",
            "format": "date_time_no_millis"
        },
        "warc_ip_address": {
            "type": "ip"
        },
        "http_date": {
            "type": "date",
            "format": "date_time_no_millis"
        },
        "http_content_length": {
            "type": "long"
        },
        "content_encoding": {
            "type": "keyword"
        },
    },
    "dynamic_templates": [
        {
            "warc_headers": {
                "match": "warc_*",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword"
                }
            },
        },
        {
            "http_headers": {
                "match": "http_*",
                "match_mapping_type": "string",
                "mapping": {
                    "type": "keyword"
                }
            }
        }
    ]
}

SETTINGS_META = {
    "codec": "best_compression",
    "refresh_interval": "-1",
    "routing.allocation.total_shards_per_node": "1"
}
