from __future__ import annotations

import asyncio
import signal
import sys

from core.platform import BridgePlatform
from utils.config_parser import load_config
from utils.config_watcher import ConfigWatcher


def main():
    config_path = "config.yaml"
    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    config = load_config(config_path)
    platform = BridgePlatform(config, config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def on_config_change():
        loop.create_task(platform.reload_config())

    watcher = ConfigWatcher(config_path, on_config_change)
    watcher.start()

    def shutdown_handler():
        print("\nShutting down...")
        watcher.stop()
        loop.create_task(platform.stop())
        loop.call_later(1, loop.stop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_handler)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(platform.start())
        loop.run_forever()
    except KeyboardInterrupt:
        watcher.stop()
        loop.run_until_complete(platform.stop())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
