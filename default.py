from __future__ import annotations

import prefect
import subprocess
import sys
import tiled


def print_argument(argument_to_print=""):
    if argument_to_print:
        print(f"argument to print: {argument_to_print}")  # noqa: T201
    else:
        print("argument to print: EMPTY")  # noqa: T201


def info():
    print(f"Prefect info: {prefect.__version_info__}")
    print(f"Tiled info: {tiled.__version__}")
    output = subprocess.check_output(["pixi", "--version"])
    print(f"Pixi info: {output.decode().strip()}")


if __name__ == "__main__":
    info()
    if len(sys.argv) > 1:
        print_argument(sys.argv[1])
    else:
        print_argument()
