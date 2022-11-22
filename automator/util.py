#!/usr/bin/env python

# General helper functions used by the coordinator 
# (Should these go in `redis_util.py`?)

import os
import requests
import json
from datetime import datetime
import time

GRAFANA_ANNOTATIONS_URL = "http://blh0:3000/api/annotations"
GRAFANA_AUTH = os.environ['GRAFANA_AUTH']

def annotate_grafana(tag, text, url=GRAFANA_ANNOTATIONS_URL, auth=GRAFANA_AUTH):
    """Create Grafana annotations.

    Args:
        tag (str): Grafana tag
        text (str): Associated description text
        url (str): Grafana annotations URL
        auth (str): Grafana auth token

    Returns:
        http POST response
    """
    header = {
        "Authorization":"Bearer {}".format(auth),
        "Accept":"application/json",
        "Content-Type":"application/json"
    }

    annotation = {
        "time":int(time.time()*1000), # Unix epoch, UTC in milliseconds
        "isRegion":False,
        "tags":[tag],
        "text":text,
    }

    return requests.post(
        url,
        headers=header,
        data=json.dumps(annotation)
    )

