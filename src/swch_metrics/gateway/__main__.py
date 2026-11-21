"""Entry point: ``swch-gateway`` (or ``python -m swch_metrics.gateway``)."""
import logging
import os

import uvicorn


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run("swch_metrics.gateway.app:app", host=os.getenv("HOST", "0.0.0.0"),
                port=int(os.getenv("PORT", "8000")), workers=1,
                root_path=os.getenv("ROOT_PATH", ""), proxy_headers=True, log_level=os.getenv("LOG_LEVEL", "info").lower())


if __name__ == "__main__":
    main()
