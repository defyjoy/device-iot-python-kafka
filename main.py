"""
Demonstrates how to schedule a job to be run in a process pool on 3 second intervals.
"""
from __future__ import annotations
import os
from devicesensor.devicesendor import DeviceSendor
from datetime import datetime


from apscheduler.schedulers.blocking import BlockingScheduler


def tick(sensor):
    # print("Tick! The time is: %s" % datetime.now())
    sensor.send()


if __name__ == "__main__":
    mySensor = DeviceSendor()
    scheduler = BlockingScheduler()
    scheduler.add_executor("processpool")
    scheduler.add_job(tick, "interval", [mySensor], seconds=2)
    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
