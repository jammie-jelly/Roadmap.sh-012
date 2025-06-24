import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List
from configs.init import logger
from operations.backup_restore import perform_backup


def schedule_backups(targets: List[Dict], target_id: str = None) -> None:
    selected_targets = [t for t in targets if target_id is None or t["id"] == target_id]
    if not selected_targets:
        raise ValueError(f"No targets found with id: {target_id}")

    def run_scheduler(target: Dict):
        intervals = {"hourly": timedelta(hours=1), "daily": timedelta(days=1), "weekly": timedelta(weeks=1)}
        interval = intervals[target["backup"]["schedule"]]
        next_run = datetime.now() + interval
        while True:
            now = datetime.now()
            if now >= next_run:
                try:
                    logger.info(f"Scheduled backup for target {target['id']}")
                    perform_backup(target)
                except Exception as e:
                    logger.error(f"Scheduled backup failed for {target['id']}: {e}")
                next_run = now + interval
            time.sleep(60)

    for target in selected_targets:
        logger.info(f"Starting scheduler for target {target['id']}")
        threading.Thread(target=run_scheduler, args=(target,), daemon=True).start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")
