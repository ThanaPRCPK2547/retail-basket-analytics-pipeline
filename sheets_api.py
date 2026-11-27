import os
import requests

RANGE = "Main Data!A1:M10001"


def _require_env(name: str) -> str:
    try:
        value = os.environ[name]
    except KeyError:
        raise RuntimeError(
            f"Required environment variable {name} is not set. "
            f"Set it in your environment, or copy .env.example to .env and load it."
        ) from None
    if not value.strip():
        raise RuntimeError(
            f"Required environment variable {name} is set but empty. "
            f"Provide a non-empty value (see .env.example)."
        )
    return value


def get_api_key() -> str:
    return _require_env("GOOGLE_API_KEY")


def get_spreadsheet_id() -> str:
    return _require_env("GOOGLE_SPREADSHEET_ID")


def build_url() -> str:
    spreadsheet_id = get_spreadsheet_id()
    api_key = get_api_key()
    return (
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
        f"/values/{RANGE}?key={api_key}"
    )


def main() -> None:
    response = requests.get(build_url())
    data = response.json()

    print(data)
    values = data.get("values", [])

    for row in values:
        print("\t".join(row))


if __name__ == "__main__":
    main()
