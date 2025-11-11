# Glacier Vault Manager — README

A compact README for `glacier_vault_manager.py` — a full-featured CLI tool to manage Amazon Glacier vaults, jobs, and archives.

---

## Prerequisites

* Python 3.8+
* Install dependencies:

```bash
pip install boto3
```

* AWS credentials configured (one of):

  * `aws configure` (recommended), or
  * Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional), or
  * Pass `--access-key`, `--secret-key` and optional `--session-token` on the command line.

---

## Where files are saved

* Saved Job IDs: `~/.glacier_jobids/<vault>_latest_jobid.txt`
* Inventory JSON: `<vault>_<jobid>_inventory.json`
* Extracted archive IDs: `<vault>_<jobid>_inventory_archive_ids.txt`
* Optional logging CSV: path passed with `--log-file` (new rows appended)

---

## Launching the program

Make the script executable or run with Python:

```bash
python3 glacier_vault_manager.py <command> [options]
```

Global options (available for all commands):

* `--region` or `-r` — AWS region (default `us-east-1`)
* `--access-key` / `--secret-key` — inline credentials (avoid in long-term use)
* `--session-token` — for temporary credentials
* `--quiet` — suppress non-error console output
* `--log-file` — path to a CSV logfile to capture actions and results

---

## Common commands & examples (expanded)

### 1) List vaults

```
python3 glacier_vault_manager.py list
```

With explicit credentials and region:

```
python3 glacier_vault_manager.py --access-key AKIA... --secret-key AbCd... --region us-east-1 list
```

### 2) Describe a vault

```
python3 glacier_vault_manager.py describe my-vault
```

### 3) Start inventory (and save JobId)

```
python3 glacier_vault_manager.py init-inventory my-vault --save-job
```

This saves the returned Job ID to `~/.glacier_jobids/my-vault_latest_jobid.txt`.

### 4) List recent jobs (and save the most recent JobId)

```
python3 glacier_vault_manager.py list-jobs my-vault --save-most-recent
```

### 5) Check job status (use saved job id)

```
python3 glacier_vault_manager.py check-job my-vault --use-saved
```

### 6) Fetch completed inventory output and extract archive IDs

Wait until the job completes then download and extract archive IDs:

```
python3 glacier_vault_manager.py fetch-inventory my-vault --use-saved --wait --poll-interval 120
```

This will create files like:

* `my-vault_E2B0bExample12345_inventory.json`
* `my-vault_E2B0bExample12345_inventory_archive_ids.txt`

If you already have the job id and don't want to wait:

```
python3 glacier_vault_manager.py fetch-inventory my-vault E2B0bExample12345
```

### 7) Delete a single archive by ID

```
python3 glacier_vault_manager.py delete-archive my-vault 3XxYyZzExampleArchiveId
```

### 8) Bulk delete archives from file (dry-run first)

Dry-run to preview:

```
python3 glacier_vault_manager.py bulk-delete my-vault archive_ids.txt --dry-run
```

Actual bulk delete (10 workers, small delay to avoid throttling):

```
python3 glacier_vault_manager.py bulk-delete my-vault archive_ids.txt --workers 10 --delay 0.2
```

Tip: combine with `--log-file ~/glacier_ops.csv` to audit deletions.

### 9) Upload a single file to Glacier (small or large)

Upload a small file (single PUT):

```
python3 glacier_vault_manager.py upload-file my-vault /path/to/backup.tar.gz
```

Upload a large file (multipart, parallel parts):

```
python3 glacier_vault_manager.py upload-file my-vault /path/to/large-backup.img --part-size 134217728 --parallel-parts 6
```

* `--part-size` is in bytes (default 100MB). Choose a power-of-two multiple of MB for best compatibility.
* `--parallel-parts` controls part upload concurrency.

### 10) Upload all files in a directory (recursive)

Dry-run to inspect files detected:

```
python3 glacier_vault_manager.py upload-dir my-vault /backups --recursive --dry-run
```

Actual upload with concurrency and CSV output:

```
python3 glacier_vault_manager.py --log-file ~/glacier_ops.csv upload-dir my-vault /backups --recursive --workers 6 --save-output uploads.csv
```

The `uploads.csv` will contain columns: `file,archiveId,error`.

### 11) Delete vault (only when empty)

After confirming vault has zero archives:

```
python3 glacier_vault_manager.py delete my-vault
```

Force attempt (not recommended unless you are sure):

```
python3 glacier_vault_manager.py delete my-vault --force
```

---

## Logging and quiet mode

* `--log-file /path/to/log.csv` appends CSV rows containing timestamp, action, vault, file, archiveId, status and message. Use this for audit.
* `--quiet` silences non-error console output (useful in scripts).

Example combining both:

```
python3 glacier_vault_manager.py --quiet --log-file ~/glacier_ops.csv upload-dir my-vault /backups --recursive --workers 8
```

---

## Troubleshooting & tips

* **Job IDs expire from Glacier job metadata** after a limited time (historically ~24 hours). If a job is older and no longer listed, start a new inventory job.
* **Start with `--dry-run`** for destructive operations (bulk-delete, upload-dir) until comfortable.
* **Multipart uploads** compute SHA256 tree-hash locally which may be CPU- and I/O-intensive for very large files.
* **Network & I/O**: uploading many large files concurrently can saturate your network or disk; reduce `--workers` or `--parallel-parts` as needed.
* **Permissions**: ensure IAM principal has the necessary Glacier permissions: `ListVaults`, `DescribeVault`, `InitiateJob`, `DescribeJob`, `GetJobOutput`, `UploadArchive`, `InitiateMultipartUpload`, `UploadMultipartPart`, `CompleteMultipartUpload`, `DeleteArchive`, `DeleteVault`.

---

## Example end-to-end cleanup workflow (expanded)

1. Start inventory and save JobId:

```bash
python3 glacier_vault_manager.py init-inventory project-vault --save-job
```

2. Periodically check status using saved JobId:

```bash
python3 glacier_vault_manager.py check-job project-vault --use-saved
```

3. Once completed, fetch inventory and extract archive IDs (wait if necessary):

```bash
python3 glacier_vault_manager.py fetch-inventory project-vault --use-saved --wait --poll-interval 300
```

4. Review archive ids (first 20):

```bash
head -n 20 project-vault_*_inventory_archive_ids.txt
```

5. Dry-run bulk delete to see what would be removed:

```bash
python3 glacier_vault_manager.py bulk-delete project-vault project-vault_*_inventory_archive_ids.txt --dry-run
```

6. Execute bulk delete with logging and moderate concurrency:

```bash
python3 glacier_vault_manager.py --log-file ~/glacier_ops.csv bulk-delete project-vault project-vault_*_inventory_archive_ids.txt --workers 8 --delay 0.25
```

7. Verify vault is empty and then delete the vault:

```bash
python3 glacier_vault_manager.py describe project-vault
python3 glacier_vault_manager.py delete project-vault
```

---

## License & safety

This tool is provided as-is. Deleting archives and vaults is irreversible — double-check before running destructive commands. Do not hardcode long-lived access keys in repositories.

---

If you’d like, I can also produce a small `example_env.sh` that exports environment variables for testing, or create a version that uploads metadata to S3 after each run. Which would you prefer next?
