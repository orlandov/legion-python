#!python

import logging
import sys

log = logging.root

def create_logger():
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(msg)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
