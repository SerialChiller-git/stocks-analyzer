import requests
import certifi
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------------
# Disable insecure warnings (for fallback SSL)
# -------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------
# CREATE SAFE SESSION
# -------------------------
session = requests.Session()

retry = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry)

session.mount("https://", adapter)
session.mount("http://", adapter)

# -------------------------
# SAFE REQUEST FUNCTION
# -------------------------
def safe_get(url, params=None):
    try:
        return session.get(
            url,
            params=params,
            timeout=10,
            verify=certifi.where()   # ✅ FIX SSL
        )

    except requests.exceptions.SSLError:
        # fallback if DSE SSL is broken
        return session.get(
            url,
            params=params,
            timeout=10,
            verify=False
        )

# -------------------------
# FETCH STOCK LIST (FIXED)
# -------------------------
def fetch_stocks():
    print("Fetching stock list...")

    url = "https://www.dsebd.org/ajax/suggestList.php"

    response = safe_get(
        url,
        params={"suggestType": "tc"}
    )

    response.raise_for_status()

    data = response.text

    # ⚠️ You may need to adjust parsing depending on actual response format
    print("Raw response preview:", data[:200])

    return data


# -------------------------
# MAIN RUN
# -------------------------
if __name__ == "__main__":
    print("🚀 Scraper started")

    stocks = fetch_stocks()

    print("Done ✔")
