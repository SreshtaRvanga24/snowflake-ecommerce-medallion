"""
Upload the Azure-sourced CSVs into the local Azurite emulator.

Usage:
    pip install azure-storage-blob
    python azurite_upload.py --src ./datasets --batch-date 2026-11-12

Creates the container if needed and lands each file as
    retail-landing/<table>/<table>_<batch-date>.csv
Existing blobs are never overwritten, so a rerun is safe.
"""
import argparse
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

# Azurite's well-known development account (public, documented by Microsoft).
# Inside Docker Compose, replace 127.0.0.1 with the service name, e.g. azurite.
AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

CONTAINER = "retail-landing"

# Source file -> landing folder. These are the tables on the Azure path.
FILES = {
    "PAYMENTS_RAW.csv": "payments",
    "ORDER_STATUS_EVENTS.csv": "order_status_events",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, help="Folder containing the CSVs")
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    service = BlobServiceClient.from_connection_string(AZURITE_CONN_STR)
    container = service.get_container_client(CONTAINER)
    try:
        container.create_container()
        print(f"Created container {CONTAINER}")
    except ResourceExistsError:
        print(f"Container {CONTAINER} already exists")

    for file_name, folder in FILES.items():
        path = Path(args.src) / file_name
        if not path.exists():
            print(f"SKIP  {file_name}: not found in {args.src}")
            continue
        blob_name = f"{folder}/{folder}_{args.batch_date}.csv"
        try:
            with path.open("rb") as fh:
                container.upload_blob(name=blob_name, data=fh, overwrite=False)
            print(f"OK    {file_name} -> {blob_name}")
        except ResourceExistsError:
            print(f"EXISTS {blob_name} (left unchanged)")

    print("\nContainer contents:")
    for blob in container.list_blobs():
        print(f"  {blob.name}  ({blob.size} bytes)")


if __name__ == "__main__":
    main()
