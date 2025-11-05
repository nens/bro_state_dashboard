import os
import time
import sys
from datetime import datetime

# File that signals the script to stop
STOP_FILE = "stop.txt"

# File that stores the last run date
LAST_RUN_FILE = "last_run.txt"

def has_run_today():
    """Check if import_data.py has already run today."""
    if not os.path.exists(LAST_RUN_FILE):
        return False
    with open(LAST_RUN_FILE, "r") as f:
        last_run_date = f.read().strip()
    today = datetime.now().strftime("%Y-%m-%d")
    return last_run_date == today

def mark_run_today():
    """Mark that import_data.py has run today."""
    today = datetime.now().strftime("%Y-%m-%d")
    with open(LAST_RUN_FILE, "w") as f:
        f.write(today)

def main():
    while True:
        if os.path.exists(STOP_FILE):
            print("Stop file detected. Exiting...")
            sys.exit(0)

        if not has_run_today():
            print("Starting data import...")
            os.system('python import_data.py')
            print("Data import finished.")
            mark_run_today()
        else:
            print("Data import already run today. Waiting...")

        # Check again in 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    main()
