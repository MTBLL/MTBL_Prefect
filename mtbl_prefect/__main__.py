"""Entrypoint for `python -m mtbl_prefect`.

Phase 0: smoke-only. Flows are added in Phase 1.
"""

import sys

import prefect


def main() -> int:
    print(f"mtbl-prefect ready. Prefect version: {prefect.__version__}")
    print("Phase 0 scaffold loaded. Flows arrive in Phase 1 (MTBL-149).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
