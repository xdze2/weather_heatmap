from pathlib import Path
import requests
import click
from itertools import cycle

PATTERN = ["_", "-", ".", "/", ":", "\\", "*", "?", '"', "<", ">", "|"]


@click.command()
# @click.option(
#     "--url-template",
#     required=True,
#     help="URL template with placeholders for dep_num and date.",
# )
# @click.option(
#     "--dep-list",
#     required=True,
#     type=click.File("r"),
#     help="Path to a file containing dep_num values, one per line.",
# )
# @click.option(
#     "--date-range",
#     required=True,
#     type=click.File("r"),
#     help="Path to a file containing date values, one per line.",
# )
def main():
    """Download files based on dep_num and date, checking if they exist first."""

    base_url = (
        "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/HOR/"
    )

    dep_list = [f"{i:02}" for i in range(1, 99)]
    date_range = "2020-2023"

    output_dir = Path("data", date_range)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for dep_num in dep_list:
        # HOR_departement_01_periode_2020
        # https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/HOR/H_01_previous-2020-2023.csv.gz
        # https://www.data.gouv.fr/fr/datasets/r/9aad7600-6ba8-47c6-b31b-79c8631d7ae9
        filename = f"H_{dep_num}_previous-{date_range}.csv.gz"
        filepath = Path(output_dir, filename)
        download_url = f"{base_url}{filename}"
        if filepath.exists():
            print(f"File {filepath} already exists. Skipping download.")
            continue

        try:
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            with open(filepath, "wb") as f:
                symbole = cycle(PATTERN)
                for chunk in response.iter_content(chunk_size=8192):
                    print(next(symbole), end="\r", flush=True)
                    f.write(chunk)
            print(f"Downloaded: {filename}")
        except requests.RequestException as e:
            print(f"Failed to download {filename}: {e}")


if __name__ == "__main__":
    main()
