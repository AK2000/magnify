from __future__ import annotations

import argparse

import daemon

from magnify.config import MonitorConfig


def main():
    """Start a resource monitor process."""
    parser = argparse.ArgumentParser(
        description='Start the monitor on a node',
        prog='python -m magnify.run',
    )
    parser.add_argument(
        '--config',
        '-c',
        required=True,
        help='Base toml configuration files to load',
    )
    parser.add_argument(
        '--foreground',
        '-f',
        help=('Start monitor process in the foreground. Default is daemon.'),
    )

    options = parser.parse_args()
    config = MonitorConfig.from_toml(options.config)

    monitor = config.get_monitor()

    if options.foreground:
        monitor.start()
        monitor.wait()

    else:
        with daemon.DaemonContext():
            monitor.start()
            monitor.wait()


if __name__ == '__main__':
    main()
