from __future__ import annotations

import logging
import os
import subprocess
import time

from common import slack


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)

logger = logging.getLogger(__file__)


DAG_SYNC_INTERVAL = os.environ.get("DAG_SYNC_INTERVAL", 10)


def run_git(*args) -> str | None:
    try:
        output = subprocess.check_output(
            args, stderr=subprocess.STDOUT, text=True
        ).strip()
        return output
    except subprocess.CalledProcessError as err:
        logger.exception(f"Git command '{args}' failed: {err}")
        return None


def main():
    while True:
        time.sleep(DAG_SYNC_INTERVAL)
        output = run_git("git", "pull", "origin", "main")
        if output is None:
            # Command failed, wait and try again
            continue
        if "Already up to date." in output:
            continue
        elif "\nFast-forward\n" in output:
            current_commit = run_git("git", "log", "--format='%s'", "-n", "1")
            if current_commit is None:
                current_commit = "<unable to determine>"
            logger.info(f"Updated DAGs to: {current_commit}")
            slack.send_message(
                f":airflow_spin: Airflow DAGs updated to: `{current_commit}`",
                username="Airflow DAG sync",
                icon_emoji=":recycle:",
            )
        else:
            logging.warning("Failed to pull DAGs")


if __name__ == "__main__":
    main()
