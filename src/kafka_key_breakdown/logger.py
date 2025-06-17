import logging
import sys

log = logging.Logger("Kafka Key Breakdown")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
log.addHandler(handler)
