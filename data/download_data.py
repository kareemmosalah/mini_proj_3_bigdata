"""
Download and extract the MovieLens 1M dataset.
Result: data/ml-1m/ratings.dat  (1,000,209 rows)
Format per row: UserID::MovieID::Rating::Timestamp
"""
import os
import sys
import urllib.request
import zipfile

DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
ZIP_PATH    = os.path.join(SCRIPT_DIR, "ml-1m.zip")
EXTRACT_DIR = SCRIPT_DIR
RATINGS_FILE = os.path.join(SCRIPT_DIR, "ml-1m", "ratings.dat")


def _progress_hook(block_num, block_size, total_size):
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(downloaded / total_size * 100, 100)
        bar = "#" * int(pct / 2)
        sys.stdout.write(f"\r  [{bar:<50}] {pct:.1f}%")
        sys.stdout.flush()


def download():
    if os.path.exists(RATINGS_FILE):
        print(f"Dataset already present at {RATINGS_FILE}")
        return

    print(f"Downloading MovieLens 1M from {DATASET_URL} ...")
    urllib.request.urlretrieve(DATASET_URL, ZIP_PATH, reporthook=_progress_hook)
    print()

    print("Extracting...")
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(EXTRACT_DIR)
    os.remove(ZIP_PATH)

    print(f"Done.  Ratings file → {RATINGS_FILE}")
    with open(RATINGS_FILE) as f:
        lines = sum(1 for _ in f)
    print(f"  Total lines (ratings): {lines:,}")


if __name__ == "__main__":
    download()
