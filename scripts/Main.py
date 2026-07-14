
import sys
import json
import time
from pathlib import Path
from curl_cffi import requests as cureq

# ==================================================
# UTF-8 CONSOLE OUTPUT
# ==================================================

sys.stdout.reconfigure(encoding="utf-8")

# ==================================================
# CONFIG
# ==================================================

START_PAGE = 1
END_PAGE = 8

OUTPUT_DIR = Path("Magickbricks\Output\Ahemdabad")

# Decodo Proxy
USERNAME = ""
PASSWORD = ""

PROXY = f"http://{USERNAME}:{PASSWORD}@gate.decodo.com:10001"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
DELAY_BETWEEN_PAGES = 5

# ==================================================
# URL TEMPLATE
# ==================================================

BASE_URL = (
    "https://www.magicbricks.com/mbsrp/propertySearch.html"
    "?editSearch=Y"
    "&category=S"
    "&propertyType=10002,10003,10021,10022,10001,10017"
    "&bedrooms=11700,11703,11704,11701,11702"
    "&city=2690"
    "&page={page}"
    "&groupstart={groupstart}"
    "&offset=0"
    "&maxOffset=2440"
    "&sortBy=premiumRecent"
    "&postedSince=-1"
    "&pType=10002,10003,10021,10022,10001,10017"
    "&isNRI=N"
    "&multiLang=en"
)

# ==================================================
# SETUP
# ==================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"[INFO] Output Folder: {OUTPUT_DIR}")
print(f"[INFO] Pages: {START_PAGE} -> {END_PAGE}")

# ==================================================
# SCRAPER
# ==================================================

for page in range(START_PAGE, END_PAGE + 1):

    groupstart = (page - 1) * 30

    url = BASE_URL.format(
        page=page,
        groupstart=groupstart
    )

    print("\n" + "=" * 70)
    print(f"[INFO] Page={page}")
    print(f"[INFO] GroupStart={groupstart}")

    success = False

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print(
                f"[INFO] Attempt {attempt}/{MAX_RETRIES}"
            )

            response = cureq.get(
                url,
                impersonate="chrome",
                proxy=PROXY,
                timeout=REQUEST_TIMEOUT
            )

            print(
                f"[INFO] Status Code: {response.status_code}"
            )
            
            response.raise_for_status()

            data = response.json()

            output_file = OUTPUT_DIR / f"page_{page:04d}.json"

            with open(
                output_file,
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(
                    data,
                    f,
                    ensure_ascii=False,
                    indent=2
                )

            print(
                f"[INFO] Saved -> {output_file.name}"
            )

            success = True
            break

        except Exception as e:

            print(
                f"[WARNING] Page {page} "
                f"Attempt {attempt}/{MAX_RETRIES} Failed"
            )
            print(f"[WARNING] {e}")

            if attempt < MAX_RETRIES:
                time.sleep(5)

    if not success:

        print(
            f"[ERROR] Failed to scrape page {page}"
        )

        with open(
            OUTPUT_DIR / "failed_pages.txt",
            "a",
            encoding="utf-8"
        ) as f:
            f.write(f"{page}\n")

    time.sleep(DELAY_BETWEEN_PAGES)

print("\n[INFO] Scraping Completed")
