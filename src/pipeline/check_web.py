import sys
import urllib.error
import urllib.request

from pipeline import config

TIMEOUT_SECONDS = 5


def fetch(url: str) -> tuple[int, str]:
    """Return the status code and first line of the body served at url."""
    with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as response:
        body = response.read().decode("utf-8", errors="replace")
        first_line = next((line for line in body.splitlines() if line.strip()), "")
        return response.status, first_line


def main() -> int:
    url = config.WEB_URL
    try:
        status, first_line = fetch(url)
    except (urllib.error.URLError, OSError) as error:
        print(f"{url} unreachable: {error}", file=sys.stderr)
        return 1

    print(f"{url} -> {status} {first_line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
