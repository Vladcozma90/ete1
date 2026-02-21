import logging

def setup_log(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s"
    )