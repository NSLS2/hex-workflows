from __future__ import annotations

import sys


def test(stuff=""):
    if stuff:
        print(f"test: {stuff}")  # noqa: T201
    else:
        print("test: EMPTY")  # noqa: T201


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test(sys.argv[1])
    else:
        test()
