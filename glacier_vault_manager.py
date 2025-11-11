#!/usr/bin/env python3
"""
glacier_vault_manager.py

Complete Glacier vault manager with full --profile support across all commands.
This version includes:
 - Correct SHA-256 tree-hash computation for uploads (fixed bug).
 - `upload-file --print-checksums` debug option to print linear SHA256 and tree-hash.
 - list_vaults limit fix (passes limit as string).
 - process-wide AWS_PROFILE enforcement when --profile provided.
 - Region handling: CLI region overrides, otherwise profile/env used.

Requires: boto3
    pip install boto3
"""
from __future__ import annotations
import argparse
import sys
import time
import csv
import json
import random
import math
import hashlib
import os
from pathlib import Path
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

# ---------------- Config ----------------
DEFAULT_REGION: Optional[str] = None  # Allow profile/env to provide region if not passed on CLI
JOBID_DIR = Path.home() / ".glacier_jobids"
JOBID_DIR.mkdir(parents=True, exist_ok=True)
GLACIER_SINGLEPUT_LIMIT = 4 * 1024 * 1024 * 1024  # 4 GiB
LOG_LOCK = threading.Lock()

# runtime
QUIET = False
_LOG_FH = None
_LOG_WRITER = None
LOG_CSV_FIELDNAMES = ["timestamp", "action", "vault", "file", "archiveId", "status", "message"]

# ---------------- Logging ----------------
def _open_log_file(path: Path):
    new = not path.exists()
    f = open(path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=LOG_CSV_FIELDNAMES)
    if new:
        writer.writeheader()
        f.flush()
    return f, writer

def init_logging(log_path: Optional[str], quiet: bool = False):
    global QUIET, _LOG_FH, _LOG_WRITER
    QUIET = quiet
    if log_path:
        p = Path(log_path).expanduser()
        _LOG_FH, _LOG_WRITER = _open_log_file(p)
        if not QUIET:
            print(f"Logging enabled -> {p}")

def close_logging():
    global _LOG_FH
    if _LOG_FH:
        try:
            _LOG_FH.close()
        except Exception:
            pass

def log_event(action: str, vault: Optional[str] = None, file: Optional[str] = None,
              archiveId: Optional[str] = None, status: str = "ok", message: Optional[str] = None):
    global _LOG_WRITER
    if _LOG_WRITER is None:
        return
    row = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "vault": vault or "",
        "file": file or "",
        "archiveId": archiveId or "",
        "status": status,
        "message": message or "",
    }
    with LOG_LOCK:
        try:
            _LOG_WRITER.writerow(row)
            _LOG_FH.flush()
        except Exception:
            pass

def info(msg: str):
    if not QUIET:
        print(msg)

def warn(msg: str):
    print(msg, file=sys.stderr)

# ---------------- AWS client creation ----------------
def get_glacier_client(region_name: Optional[str] = DEFAULT_REGION,
                       access_key: Optional[str] = None,
                       secret_key: Optional[str] = None,
                       session_token: Optional[str] = None,
                       profile: Optional[str] = None,
                       timeout_seconds: Optional[int] = None):
    """
    Create a boto3 Session and Glacier client.
    region_name may be None so boto3 session/profile/env determines final region.
    """
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()
    config = Config(connect_timeout=timeout_seconds, read_timeout=timeout_seconds) if timeout_seconds else None
    if access_key and secret_key:
        return session.client("glacier",
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              aws_session_token=session_token,
                              region_name=region_name,
                              config=config)
    return session.client("glacier", region_name=region_name, config=config)

# ---------------- Job ID helpers ----------------
def jobid_file_for(vault: str) -> Path:
    safe = vault.replace("/", "_")
    return JOBID_DIR / f"{safe}_latest_jobid.txt"

def save_jobid(vault: str, jobid: str):
    p = jobid_file_for(vault)
    p.write_text(jobid)
    info(f"Saved JobId to {p}")
    log_event("save_jobid", vault=vault, message=jobid)

def load_saved_jobid(vault: str) -> Optional[str]:
    p = jobid_file_for(vault)
    if p.exists():
        return p.read_text().strip()
    return None

# ---------------- Vault operations ----------------
def list_vaults(region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, limit: Optional[int] = None, timeout_seconds: Optional[int] = None):
    """
    List Glacier vaults. Converts `limit` to str before sending (Glacier expects string).
    region may be None -> boto3/session/profile/env used.
    """
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    vaults = []
    marker = None
    try:
        while True:
            kwargs = {}
            if marker:
                kwargs["marker"] = marker
            if limit is not None:
                kwargs["limit"] = str(limit)
            resp = client.list_vaults(**kwargs)
            vaults.extend(resp.get("VaultList", []))
            marker = resp.get("Marker")
            if not marker or limit is not None:
                break
        log_event("list_vaults", message=f"count={len(vaults)}")
        return vaults
    except ClientError as e:
        log_event("list_vaults", status="error", message=str(e))
        raise

def describe_vault(vault: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, timeout_seconds: Optional[int] = None):
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    return client.describe_vault(vaultName=vault)

def delete_vault(vault: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, force: bool = False, timeout_seconds: Optional[int] = None):
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    desc = client.describe_vault(vaultName=vault)
    num = desc.get("NumberOfArchives", 0)
    if num and not force:
        log_event("delete_vault", vault=vault, status="error", message=f"not_empty:{num}")
        raise RuntimeError(f"Vault '{vault}' not empty ({num} archives). Use --force to override.")
    client.delete_vault(vaultName=vault)
    info(f"Deleted vault {vault}")
    log_event("delete_vault", vault=vault, status="ok")

# ---------------- Jobs & Inventory ----------------
def initiate_inventory(vault: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, save_job: bool = False, timeout_seconds: Optional[int] = None) -> str:
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    resp = client.initiate_job(vaultName=vault, jobParameters={"Type": "inventory-retrieval"})
    jobid = resp.get("jobId")
    info(f"Started inventory job {jobid}")
    log_event("initiate_inventory", vault=vault, message=jobid)
    if save_job and jobid:
        save_jobid(vault, jobid)
    return jobid

def list_jobs(vault: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, save_most_recent: bool = False, timeout_seconds: Optional[int] = None):
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    resp = client.list_jobs(vaultName=vault)
    jobs = resp.get("JobList", [])
    jobs_sorted = sorted(jobs, key=lambda j: j.get("CreationDate") or "", reverse=True)
    log_event("list_jobs", vault=vault, message=f"count={len(jobs_sorted)}")
    if save_most_recent and jobs_sorted:
        save_jobid(vault, jobs_sorted[0].get("JobId"))
    return jobs_sorted

def describe_job(vault: str, jobid: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, timeout_seconds: Optional[int] = None):
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    resp = client.describe_job(vaultName=vault, jobId=jobid)
    log_event("describe_job", vault=vault, message=json.dumps({"jobId": jobid, "Completed": resp.get("Completed")}))
    return resp

def fetch_inventory_output(vault: str, jobid: str, region: Optional[str], output: Optional[str] = None, wait: bool = False, poll_interval: int = 60, access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, save_archive_ids: bool = True, timeout_seconds: Optional[int] = None) -> str:
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    if wait:
        info(f"Waiting for job {jobid} to complete (poll every {poll_interval}s)...")
        while True:
            d = describe_job(vault, jobid, region, access_key, secret_key, session_token, profile, timeout_seconds)
            if d.get("Completed"):
                info("Job completed.")
                break
            time.sleep(poll_interval)
    if not output:
        safe = vault.replace("/", "_")
        output = f"{safe}_{jobid}_inventory.json"
    with open(output, "wb") as fh:
        resp = client.get_job_output(vaultName=vault, jobId=jobid)
        stream = resp["body"]
        while True:
            chunk = stream.read(1024 * 64)
            if not chunk:
                break
            fh.write(chunk)
    info(f"Saved job output to {output}")
    log_event("fetch_inventory_output", vault=vault, file=output)
    if save_archive_ids:
        try:
            with open(output, "r", encoding="utf-8") as f:
                data = json.load(f)
            alist = data.get("ArchiveList", [])
            ids = [a.get("ArchiveId") for a in alist if a.get("ArchiveId")]
            if ids:
                aid_file = Path(output).with_suffix("")
                aid_file = Path(str(aid_file) + "_archive_ids.txt")
                with open(aid_file, "w", encoding="utf-8") as g:
                    for _id in ids:
                        g.write(_id + "\n")
                info(f"Wrote {len(ids)} archive ids to {aid_file}")
                log_event("extract_archive_ids", vault=vault, file=str(aid_file), message=f"count={len(ids)}")
        except Exception as e:
            log_event("extract_archive_ids", vault=vault, status="error", message=str(e))
            warn(f"Failed to parse inventory JSON: {e}")
    return output

# ---------------- Archive Operations ----------------
def delete_archive(vault: str, archive_id: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, timeout_seconds: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    try:
        client.delete_archive(vaultName=vault, archiveId=archive_id)
        log_event("delete_archive", vault=vault, archiveId=archive_id, status="ok")
        info(f"Deleted archive {archive_id}")
        return True, None
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ResourceNotFoundException":
            log_event("delete_archive", vault=vault, archiveId=archive_id, status="not_found")
            return False, "not_found"
        log_event("delete_archive", vault=vault, archiveId=archive_id, status="error", message=str(e))
        return False, str(e)

def _read_archive_ids(file_path: str) -> List[str]:
    ids = []
    with open(file_path, newline="") as f:
        first = f.readline()
        f.seek(0)
        if "," in first:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                val = row[0].strip()
                if val.lower() in {"archiveid", "archive_id", "id"}:
                    continue
                ids.append(val)
        else:
            ids = [line.strip() for line in f if line.strip()]
    return ids

def _delete_with_retry(vault, aid, region, access_key, secret_key, session_token, profile, retries=4, base_delay=1.0):
    for attempt in range(retries + 1):
        ok, err = delete_archive(vault, aid, region, access_key, secret_key, session_token, profile)
        if ok:
            return aid, True, "deleted"
        if err and "not_found" in err:
            return aid, False, "not_found"
        time.sleep(base_delay * (2 ** attempt) + random.uniform(0, 0.5))
    return aid, False, "failed"

def bulk_delete_archives(vault: str, file_path: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, workers: int = 5, delay: float = 0.0, retries: int = 4, dry_run: bool = False, timeout_seconds: Optional[int] = None):
    ids = _read_archive_ids(file_path)
    total = len(ids)
    if total == 0:
        info("No archive ids found.")
        return
    info(f"Loaded {total} archive ids from {file_path}")
    if dry_run:
        info("Dry run: first 20 ids:")
        for i in ids[:20]:
            info(f" - {i}")
        return
    deleted = failed = not_found = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_delete_with_retry, vault, aid, region, access_key, secret_key, session_token, profile, retries): aid for aid in ids}
        for i, fut in enumerate(as_completed(futures), 1):
            aid, ok, msg = fut.result()
            if ok:
                deleted += 1
            elif msg == "not_found":
                not_found += 1
            else:
                failed += 1
            info(f"Progress: {i}/{total} deleted:{deleted} not_found:{not_found} failed:{failed}")
            if delay:
                time.sleep(delay)
    info(f"Bulk delete done. deleted:{deleted} not_found:{not_found} failed:{failed}")
    log_event("bulk_delete", vault=vault, message=f"total={total},deleted={deleted},failed={failed},not_found={not_found}")

# ---------------- Upload helpers (corrected tree-hash) ----------------
def _stream_sha256_tree_hash_file(path: str, chunk_size: int = 1024 * 1024) -> Tuple[str, str, int]:
    """
    Compute (linear_sha256_hex, tree_hash_hex, total_size).
    Uses chunking (default 1 MiB) and computes tree hash as AWS Glacier expects.
    Important: final tree hash is the hex of the final digest (do NOT hash it again).
    """
    linear = hashlib.sha256()
    chunk_hashes: List[bytes] = []
    total = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            total += len(chunk)
            linear.update(chunk)
            chunk_hashes.append(hashlib.sha256(chunk).digest())

    # If file empty
    if not chunk_hashes:
        tree_hash = hashlib.sha256(b"").hexdigest()
        return linear.hexdigest(), tree_hash, total

    # Reduce pairwise
    while len(chunk_hashes) > 1:
        next_round: List[bytes] = []
        for i in range(0, len(chunk_hashes), 2):
            if i + 1 < len(chunk_hashes):
                next_round.append(hashlib.sha256(chunk_hashes[i] + chunk_hashes[i + 1]).digest())
            else:
                next_round.append(chunk_hashes[i])
        chunk_hashes = next_round

    final_tree_digest = chunk_hashes[0]  # already raw digest bytes
    tree_hash_hex = final_tree_digest.hex()
    return linear.hexdigest(), tree_hash_hex, total

def upload_file_archive(vault: str, file_path: str, region: Optional[str],
                        access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None,
                        archive_description: Optional[str] = None, part_size: int = 100 * 1024 * 1024, max_retries: int = 3, parallel_parts: int = 4, timeout_seconds: Optional[int] = None, print_checksums: bool = False) -> str:
    """
    Upload a single file to Glacier. If print_checksums is True, compute and print hashes and return without uploading.
    """
    p = Path(file_path)
    if not p.is_file():
        raise FileNotFoundError(f"{file_path} not found or not a regular file")

    linear, tree_hash, total_size = _stream_sha256_tree_hash_file(file_path)
    info(f"Computed sizes: bytes={total_size}")
    if print_checksums:
        print(f"file: {file_path}")
        print(f"size: {total_size}")
        print(f"linear-sha256: {linear}")
        print(f"tree-hash: {tree_hash}")
        return ""

    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    desc = archive_description or p.name
    info(f"Preparing upload {file_path}")
    if total_size <= GLACIER_SINGLEPUT_LIMIT:
        attempt = 0
        while attempt <= max_retries:
            try:
                with open(file_path, "rb") as fh:
                    resp = client.upload_archive(vaultName=vault, body=fh, archiveDescription=desc, checksum=tree_hash)
                archive_id = resp.get("archiveId")
                info(f"Uploaded {file_path} -> {archive_id}")
                log_event("upload_file", vault=vault, file=file_path, archiveId=archive_id)
                return archive_id
            except ClientError as e:
                attempt += 1
                warn(f"upload_archive failed attempt {attempt}: {e}")
                if attempt > max_retries:
                    log_event("upload_file", vault=vault, file=file_path, status="error", message=str(e))
                    raise
                time.sleep((2 ** attempt) + random.uniform(0, 1))

    # multipart path
    if part_size < 1 * 1024 * 1024:
        part_size = 1 * 1024 * 1024
    num_parts = math.ceil(total_size / part_size)
    info(f"Multipart upload: {num_parts} parts (part_size={part_size})")
    client = get_glacier_client(region, access_key, secret_key, session_token, profile, timeout_seconds)
    mpu = client.initiate_multipart_upload(vaultName=vault, archiveDescription=desc, partSize=str(part_size))
    upload_id = mpu.get("uploadId")
    parts = []
    offset = 0
    for part_index in range(int(num_parts)):
        start = offset
        end = min(offset + part_size, total_size) - 1
        parts.append((part_index, start, end))
        offset += part_size

    def _upload_part(part_tuple):
        idx, start, end = part_tuple
        size = end - start + 1
        byte_range = f"bytes {start}-{end}/*"
        attempt = 0
        while attempt <= max_retries:
            try:
                with open(file_path, "rb") as fh:
                    fh.seek(start)
                    chunk = fh.read(size)
                resp = client.upload_multipart_part(vaultName=vault, uploadId=upload_id, range=byte_range, body=chunk)
                checksum = resp.get("checksum")
                info(f"Uploaded part {idx} ({start}-{end}) checksum={checksum}")
                return idx, True, checksum
            except ClientError as e:
                attempt += 1
                warn(f"Part {idx} upload failed attempt {attempt}: {e}")
                if attempt > max_retries:
                    return idx, False, str(e)
                time.sleep((2 ** attempt) + random.uniform(0, 1))

    with ThreadPoolExecutor(max_workers=parallel_parts) as ex:
        futures = {ex.submit(_upload_part, p): p for p in parts}
        results = [None] * len(parts)
        for f in as_completed(futures):
            idx, ok, data = f.result()
            results[idx] = (idx, ok, data)

    failed_parts = [r for r in results if not r[1]]
    if failed_parts:
        warn(f"Failed parts: {failed_parts}. Aborting multipart.")
        try:
            client.abort_multipart_upload(vaultName=vault, uploadId=upload_id)
        except Exception:
            pass
        log_event("upload_file_mpu", vault=vault, file=file_path, status="error", message=f"failed_parts={len(failed_parts)}")
        raise RuntimeError(f"Multipart upload failed for {len(failed_parts)} parts")

    resp = client.complete_multipart_upload(vaultName=vault, uploadId=upload_id, archiveSize=str(total_size), checksum=tree_hash)
    archive_id = resp.get("archiveId")
    info(f"Completed multipart upload -> {archive_id}")
    log_event("upload_file_mpu", vault=vault, file=file_path, archiveId=archive_id)
    return archive_id

def upload_dir_archives(vault: str, dir_path: str, region: Optional[str], access_key=None, secret_key=None, session_token=None, profile: Optional[str] = None, recursive: bool = True, workers: int = 3, dry_run: bool = False, save_output: Optional[str] = None, description_prefix: Optional[str] = None, max_retries: int = 3, parallel_parts: int = 4, timeout_seconds: Optional[int] = None):
    base = Path(dir_path)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"{dir_path} not a dir")
    files = []
    if recursive:
        for p in base.rglob("*"):
            if p.is_file():
                files.append(p)
    else:
        for p in base.iterdir():
            if p.is_file():
                files.append(p)
    if not files:
        info("No files found")
        return []
    info(f"Found {len(files)} files under {dir_path}")
    if dry_run:
        info("DRY RUN - files to upload:")
        for f in files:
            info(f" - {f}")
        return [(str(f), None, "dry-run") for f in files]
    results = []
    def _worker(p: Path):
        rel = str(p.relative_to(base))
        desc = f"{description_prefix}/{rel}" if description_prefix else rel
        attempt = 0
        last_err = None
        while attempt <= max_retries:
            try:
                aid = upload_file_archive(vault, str(p), region, access_key, secret_key, session_token, profile, archive_description=desc, part_size=100*1024*1024, max_retries=max_retries, parallel_parts=parallel_parts, timeout_seconds=timeout_seconds)
                return str(p), aid, None
            except Exception as e:
                last_err = str(e)
                attempt += 1
                warn(f"Upload {p} failed attempt {attempt}: {e}")
                time.sleep((2 ** attempt) + random.uniform(0, 1))
        return str(p), None, last_err
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_worker, p): p for p in files}
        for f in as_completed(futures):
            fpath, aid, err = f.result()
            results.append((fpath, aid, err))
            status = "uploaded" if aid else f"failed: {err}"
            info(f"{fpath} -> {status}")
            if aid:
                log_event("upload_dir_file", vault=vault, file=fpath, archiveId=aid)
            else:
                log_event("upload_dir_file", vault=vault, file=fpath, status="error", message=str(err))
    if save_output:
        with open(save_output, "w", newline="", encoding="utf-8") as outf:
            writer = csv.writer(outf)
            writer.writerow(["file", "archiveId", "error"])
            for r in results:
                writer.writerow(r)
        info(f"Wrote upload results to {save_output}")
    return results

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(description="AWS Glacier Vault Manager")
    p.add_argument("--region", "-r", default=None, help="AWS region to use. If omitted, use profile/env region.")
    p.add_argument("--profile", help="AWS CLI profile to use (preferred)")
    p.add_argument("--access-key", help="AWS access key (not recommended in long-term)")
    p.add_argument("--secret-key", help="AWS secret key")
    p.add_argument("--session-token", help="AWS session token (temporary creds)")
    p.add_argument("--quiet", action="store_true", help="Suppress non-error output")
    p.add_argument("--log-file", help="CSV logfile path to append operations")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List vaults")

    d = sub.add_parser("describe", help="Describe a vault")
    d.add_argument("vault")

    i = sub.add_parser("init-inventory", help="Initiate inventory retrieval")
    i.add_argument("vault")
    i.add_argument("--save-job", action="store_true")

    lj = sub.add_parser("list-jobs", help="List jobs for a vault")
    lj.add_argument("vault")
    lj.add_argument("--save-most-recent", action="store_true")

    cj = sub.add_parser("check-job", help="Describe a job")
    cj.add_argument("vault")
    cj.add_argument("jobid", nargs="?")
    cj.add_argument("--use-saved", action="store_true")

    fi = sub.add_parser("fetch-inventory", help="Fetch job output (inventory)")
    fi.add_argument("vault")
    fi.add_argument("jobid", nargs="?")
    fi.add_argument("--use-saved", action="store_true")
    fi.add_argument("--wait", action="store_true")
    fi.add_argument("--poll-interval", type=int, default=60)
    fi.add_argument("--output", help="Output JSON filename")

    delp = sub.add_parser("delete", help="Delete an empty vault")
    delp.add_argument("vault")
    delp.add_argument("--force", action="store_true")

    da = sub.add_parser("delete-archive", help="Delete a single archive by id")
    da.add_argument("vault")
    da.add_argument("archiveid")

    bd = sub.add_parser("bulk-delete", help="Bulk delete archives from file")
    bd.add_argument("vault")
    bd.add_argument("file")
    bd.add_argument("--workers", "-w", type=int, default=5)
    bd.add_argument("--delay", "-d", type=float, default=0.0)
    bd.add_argument("--retries", type=int, default=4)
    bd.add_argument("--dry-run", action="store_true")

    uf = sub.add_parser("upload-file", help="Upload a single file to a vault")
    uf.add_argument("vault")
    uf.add_argument("path")
    uf.add_argument("--description", help="Archive description")
    uf.add_argument("--part-size", type=int, default=100 * 1024 * 1024)
    uf.add_argument("--parallel-parts", type=int, default=4)
    uf.add_argument("--print-checksums", action="store_true", help="Compute and print linear SHA256 and tree-hash for the file, then exit (no upload)")

    ud = sub.add_parser("upload-dir", help="Upload every file in a directory as archives")
    ud.add_argument("vault")
    ud.add_argument("dir")
    ud.add_argument("--recursive", action="store_true", default=False)
    ud.add_argument("--workers", "-w", type=int, default=3)
    ud.add_argument("--dry-run", action="store_true")
    ud.add_argument("--save-output", help="CSV file to save results")
    ud.add_argument("--description-prefix", help="Prefix for archive description")
    ud.add_argument("--max-retries", type=int, default=3)
    ud.add_argument("--parallel-parts", type=int, default=4)

    sc = sub.add_parser("self-check", help="Validate active AWS credentials/profile by making a lightweight call")
    sc.add_argument("--timeout", type=int, default=10, help="Timeout seconds for the check (default 10)")

    return p.parse_args()

# ---------------- Main ----------------
def main():
    args = parse_args()

    # Ensure profile is honored process-wide
    if getattr(args, "profile", None):
        os.environ['AWS_PROFILE'] = args.profile

    init_logging(args.log_file, quiet=args.quiet)
    try:
        if args.cmd == "list":
            vaults = list_vaults(args.region, args.access_key, args.secret_key, args.session_token, args.profile)
            for v in vaults:
                info(f"- {v['VaultName']} ({v.get('NumberOfArchives',0)} archives, {v.get('SizeInBytes',0)} bytes)")
        elif args.cmd == "describe":
            desc = describe_vault(args.vault, args.region, args.access_key, args.secret_key, args.session_token, args.profile)
            print(json.dumps(desc, indent=2, default=str))
        elif args.cmd == "init-inventory":
            jobid = initiate_inventory(args.vault, args.region, args.access_key, args.secret_key, args.session_token, args.profile, save_job=args.save_job)
            info(f"Started job: {jobid}")
        elif args.cmd == "list-jobs":
            jobs = list_jobs(args.vault, args.region, args.access_key, args.secret_key, args.session_token, args.profile, save_most_recent=args.save_most_recent)
            for j in jobs:
                info(f"- JobId: {j.get('JobId')} Action:{j.get('Action')} Status:{j.get('StatusCode')}")
        elif args.cmd == "check-job":
            jobid = args.jobid
            if not jobid and args.use_saved:
                jobid = load_saved_jobid(args.vault)
                if not jobid:
                    raise RuntimeError("No saved job id for vault")
                info(f"Using saved job id {jobid}")
            desc = describe_job(args.vault, jobid, args.region, args.access_key, args.secret_key, args.session_token, args.profile)
            print(json.dumps(desc, indent=2, default=str))
        elif args.cmd == "fetch-inventory":
            jobid = args.jobid
            if not jobid and args.use_saved:
                jobid = load_saved_jobid(args.vault)
                if not jobid:
                    raise RuntimeError("No saved job id for vault")
                info(f"Using saved job id {jobid}")
            out = fetch_inventory_output(args.vault, jobid, args.region, output=args.output, wait=args.wait, poll_interval=args.poll_interval, access_key=args.access_key, secret_key=args.secret_key, session_token=args.session_token, profile=args.profile)
            info(f"Inventory saved to {out}")
        elif args.cmd == "delete":
            delete_vault(args.vault, args.region, args.access_key, args.secret_key, args.session_token, args.profile, force=args.force)
        elif args.cmd == "delete-archive":
            ok, err = delete_archive(args.vault, args.archiveid, args.region, args.access_key, args.secret_key, args.session_token, args.profile)
            if not ok:
                raise RuntimeError(f"Delete archive failed: {err}")
        elif args.cmd == "bulk-delete":
            bulk_delete_archives(args.vault, args.file, args.region, access_key=args.access_key, secret_key=args.secret_key, session_token=args.session_token, profile=args.profile, workers=args.workers, delay=args.delay, retries=args.retries, dry_run=args.dry_run)
        elif args.cmd == "upload-file":
            # If --print-checksums, compute & print then exit
            if getattr(args, "print_checksums", False):
                upload_file_archive(args.vault, args.path, args.region, access_key=args.access_key, secret_key=args.secret_key, session_token=args.session_token, profile=args.profile, print_checksums=True)
            else:
                aid = upload_file_archive(args.vault, args.path, args.region, access_key=args.access_key, secret_key=args.secret_key, session_token=args.session_token, profile=args.profile, archive_description=args.description, part_size=args.part_size, parallel_parts=args.parallel_parts)
                info(f"Uploaded -> {aid}")
        elif args.cmd == "upload-dir":
            results = upload_dir_archives(args.vault, args.dir, args.region, access_key=args.access_key, secret_key=args.secret_key, session_token=args.session_token, profile=args.profile, recursive=args.recursive, workers=args.workers, dry_run=args.dry_run, save_output=args.save_output, description_prefix=args.description_prefix, max_retries=args.max_retries, parallel_parts=args.parallel_parts)
            succ = sum(1 for r in results if r[1])
            total = len(results)
            info(f"Uploaded {succ}/{total}")
        elif args.cmd == "self-check":
            timeout = getattr(args, "timeout", 10)
            try:
                vaults = list_vaults(args.region, args.access_key, args.secret_key, args.session_token, args.profile, limit=1, timeout_seconds=timeout)
                info("Self-check succeeded. Able to call Glacier API.")
                if vaults:
                    info(f"Found vault (sample): {vaults[0].get('VaultName')}")
                else:
                    info("No vaults found (account may have none).")
                log_event("self_check", message="ok")
            except NoCredentialsError:
                warn("Self-check failed: no AWS credentials found.")
                log_event("self_check", status="error", message="no_credentials")
                sys.exit(2)
            except Exception as e:
                warn(f"Self-check failed: {e}")
                log_event("self_check", status="error", message=str(e))
                sys.exit(3)
        else:
            raise RuntimeError("Unknown command")
    except KeyboardInterrupt:
        warn("Cancelled by user")
    except NoCredentialsError:
        warn("No AWS credentials found. Use --profile or configure credentials.")
    except Exception as e:
        warn(f"Error: {e}")
    finally:
        close_logging()

if __name__ == "__main__":
    main()
