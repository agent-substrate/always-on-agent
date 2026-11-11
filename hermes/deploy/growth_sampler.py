#!/usr/bin/env python3
"""Per-agent snapshot growth and restore latency, as a time series.

This is the pass 2 instrumentation we owe Nous in writing: does an agent that has
been running for weeks cost more to suspend and more to restore than one that
started yesterday, and if so which part of it grows.

**Why a separate tool, and why it has to start before the soak does.** The
question is a *trend*, so the only irreplaceable measurement is the first one.
Every later sample can be taken again tomorrow; the datapoint from an agent with
one day of history cannot be recovered once the agent has three weeks of history.
So `once` runs immediately after the fleet is deployed, before the first turn,
and `sample` runs for the length of the soak alongside it.

## The four quantities, and where each actually comes from

  snapshot size    per-object listing of the actor's own snapshot prefix
  DurableDir size  the `durable-dir.tar.zstd` object in that same listing
  restore latency  RESUMING -> RUNNING, observed by polling the control plane
  skill count      tar headers of durable-dir.tar.zstd, read without a download
                   of the memory image and without waking the agent

Three of the four fall out of one object listing per agent, which is one API call
each. Nothing here wakes an agent, takes a turn, or touches the gateway: an
instrument that perturbs the duty cycle it is measuring would invalidate the soak
it is attached to.

**The collector's `atelet_snapshot_size_bytes` histogram is NOT this number and
must not be quoted as it.** Two reasons, either one fatal. It is labelled by
template, and all ten agents share one template, so it cannot be split per agent
at all. And it records the *uncompressed* size ateom wrote to local disk, while
what lands in the bucket is zstd. On the pass 1 bucket the stored image is ~45MB
an object; the uncompressed figure is a different quantity with a different use
(it tracks decompression work at restore, not storage). Both are worth having.
They are not interchangeable and this file records the stored one.

## What the restore numbers are, precisely

Polling cannot see a transition it did not sample, so every restore here is
reported as an interval rather than a point:

    lower_s     last observation in RESUMING to first observation in RUNNING
    observed_s  first observation in RESUMING to first observation in RUNNING
    upper_s     the poll before RESUMING was first seen, to first seen RUNNING

`observed_s` is the point estimate and `upper_s - lower_s` is at most two poll
intervals. A resume faster than one interval is seen as SUSPENDED -> RUNNING with
no RESUMING in between; that is recorded with `missed_intermediate: true`,
`lower_s: 0`, and it is a *bound*, not a measurement. If most of the fleet's
restores come back bounded rather than measured, the interval is too coarse for
the machine and should come down, which is exactly what the report says.

This is deliberately a second, independent instrument rather than a refinement of
the gateway-side number in `t15_baseline.py`. That one measures what a user waits
for, model call included. This one measures the platform's restore alone. They
answer different questions and neither substitutes for the other, so the report
prints the collector's fleet-wide restore histogram beside the polled per-agent
numbers: if the two disagree on the fleet median, the poller is wrong and the
per-agent series should not be believed.

## Usage

    # port-forward the collector first if you want the reconciliation series
    kubectl --context <ctx> -n otel-system port-forward svc/opentelemetry-collector 18889:8889

    ./growth_sampler.py once   --run pass2 --note "fleet deployed, before first turn"
    ./growth_sampler.py sample --run pass2                 # leave running for the soak
    ./growth_sampler.py report --run pass2

Everything is appended to `<out>/growth-<run>.jsonl` as it happens, so a
Ctrl-C, a laptop lid or a killed port-forward costs the samples not yet taken and
nothing already taken. `report` is offline and can be run at any point mid-soak.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

CONTEXT = os.environ.get(
    "KUBECTL_CONTEXT", "REPLACE_WITH_KUBECTL_CONTEXT")
ATESPACE = os.environ.get("ATESPACE", "hermes-pass2")
KUBECTL_ATE = os.environ.get("KUBECTL_ATE", "kubectl-ate")

# The object ateom writes the durable-dir volumes into, plus the `.zstd` atelet
# appends on upload. Kept as a prefix match rather than an exact name so a change
# to the compression suffix degrades to "counted in the total" instead of
# "silently reported as zero DurableDir".
DURABLE_OBJECT = "durable-dir.tar"

# Restore latency, fleet-wide, for the cross-check against the polled numbers.
# `_sum`/`_count` on the total phase only; the per-stage observations share the
# family and adding them in reports several times the restores that happened.
RESTORE_HISTOGRAM = "ate_actor_restore_duration_seconds"

RUNNING = "ACTOR_STATE_RUNNING"
RESUMING = "ACTOR_STATE_RESUMING"
SUSPENDED = "ACTOR_STATE_SUSPENDED"


# --------------------------------------------------------------------------
# control plane
# --------------------------------------------------------------------------

def ate(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run([KUBECTL_ATE, "--context", CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


def actors() -> list[dict]:
    """Every actor in the atespace, as the control plane currently sees it.

    Raises on failure rather than returning empty. An empty list is a legitimate
    answer (the fleet is not deployed yet) and a poller that conflates it with
    "the API server did not answer" records ten agents vanishing and coming back,
    which the transition tracker would then report as ten restores.
    """
    p = ate("get", "actors", "-a", ATESPACE, "-o", "json")
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout).strip() or "kubectl-ate failed")
    doc = json.loads(p.stdout)
    return doc.get("actors", []) if isinstance(doc, dict) else doc


def snapshot_of(doc: dict) -> str:
    """The actor's snapshot prefix, across two incompatible client builds.

    `ActorStatus` field 4 was `ObjectRef latest_snapshot` and is now
    `ExternalSnapshot external_snapshot`. The tag was reused rather than
    retired, so a kubectl-ate built before the change decodes the new message
    against the old descriptor: `ExternalSnapshot.snapshot_uri` and
    `ObjectRef.atespace` are both string field 1, so the URI parses cleanly and
    is printed under `status.latestSnapshot.atespace`. Field 2 is an enum on the
    wire against a string in the old message, so it is dropped as unknown and
    nothing errors anywhere.

    The result is a snapshot URI presented as an atespace name, silently. Read
    both shapes, and only trust the legacy one when it holds something that is
    actually a URI, because a genuine `ObjectRef.atespace` is a short name.
    """
    st = doc.get("status") or {}
    uri = (st.get("externalSnapshot") or {}).get("snapshotUri", "")
    if uri:
        return uri
    legacy = (st.get("latestSnapshot") or {}).get("atespace", "")
    return legacy if "://" in legacy else ""


def worker_of(doc: dict) -> str:
    wa = (doc.get("status") or {}).get("workerAssignment") or {}
    return wa.get("workerPod") or (wa.get("worker") or {}).get("name", "")


def digest(doc: dict) -> dict:
    md = doc.get("metadata") or {}
    return {
        "actor": md.get("name", "?"),
        "uid": md.get("uid", ""),
        "state": (doc.get("status") or {}).get("state") or "",
        "worker": worker_of(doc),
        "snapshot_uri": snapshot_of(doc),
    }


# --------------------------------------------------------------------------
# object store
# --------------------------------------------------------------------------

def list_objects(uri: str, timeout: int = 120) -> list[dict]:
    """Objects under a snapshot prefix, as [{"name","bytes"}].

    One `gcloud storage ls --json <uri>/**` per agent, which is one API call: the
    double-star is expanded server side into a flat listing. `metadata.size` is a
    JSON *string* and int() is not optional.

    A missing prefix is an empty list, not an error. Between an actor's creation
    and its first suspend there is genuinely no snapshot, and a sampler that
    treats that as a failure has nothing to say about the first hour of a soak,
    which is the part we cannot re-measure.
    """
    if not uri:
        return []
    p = subprocess.run(
        ["gcloud", "storage", "ls", "--json", uri.rstrip("/") + "/**"],
        capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        err = (p.stderr or "").strip()
        if "not found" in err.lower() or "matched no objects" in err.lower():
            return []
        raise RuntimeError(f"listing {uri}: {err[:400]}")
    out = []
    for entry in json.loads(p.stdout or "[]"):
        md = entry.get("metadata") or {}
        name = md.get("name") or entry.get("url", "")
        size = md.get("size")
        if size is None:
            continue                      # a prefix, not an object
        out.append({"name": name.rsplit("/", 1)[-1], "bytes": int(size)})
    return out


def size_record(d: dict, objects: list[dict]) -> dict:
    """Split a listing into the numbers the report is about.

    `durable_bytes` is the DurableDir, `image_bytes` is the memory image, and
    `manifest.json` is neither: it is a few hundred bytes of asset references and
    counting it as image would make a data-only snapshot look like it still had
    one. They are separated because durable data and memory image grow for
    different reasons and at different rates: durable with what the agent has
    written, image with what its process is holding open.

    **`scope` is load-bearing, and the live rel01 install is what taught us.** A
    template with `onCommit: DATA` writes snapshots holding only the durable tar,
    so an actor sampled just after a commit has no memory image *at all*. Folding
    that sample into a total-size series reports the image collapsing to nothing
    and recovering, which is a graph of the snapshot policy rather than of
    anything the agent did. The report separates the series on this field.
    """
    durable = sum(o["bytes"] for o in objects if o["name"].startswith(DURABLE_OBJECT))
    manifest = sum(o["bytes"] for o in objects if o["name"] == "manifest.json")
    total = sum(o["bytes"] for o in objects)
    image = total - durable - manifest
    return {
        "kind": "sizes", "t": time.time(), **d,
        "objects": objects,
        "object_count": len(objects),
        "total_bytes": total,
        "durable_bytes": durable,
        "image_bytes": image,
        "manifest_bytes": manifest,
        "scope": "full" if image else "data",
    }


# --- Substrate's sparse-extent container -----------------------------------
#
# Snapshot objects named `*.zstd` are NOT zstd streams. `zstd -d` refuses them
# and tar sees garbage. They are the sparse-extent format from
# cmd/atelet/internal/ategcs/sparsezstd.go:
#
#   magic["ATESPRSE"] | version:u32 | zstd( total:i64 | (off:i64,len:i64,data)* | -1:i64 )
#
# Magic and version are in the clear, everything after is one zstd stream
# carrying only the populated extents, all integers little-endian. A guest memory
# image is mostly holes, so this is the difference between compressing the
# logical image and compressing the resident set.
#
# The writer only reaches for the container when it has a sparse file, so a plain
# zstd stream is legal too and is passed through.
#
# Decode the snapshot container format without external analysis tools.
SPARSE_MAGIC = b"ATESPRSE"
SPARSE_VERSION = 2


def read_exactly(stream, n: int) -> bytes:
    """Reads n bytes, or everything left if the stream ends first.

    A pipe opened unbuffered returns short reads, and both readers below take the
    length of what came back as evidence, so a short read that is not retried
    looks like a truncated object rather than a slow one.
    """
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


def sparse_logical_bytes(uri: str, prefix_bytes: int = 262144,
                         timeout: int = 120) -> int | None:
    """Logical size of a snapshot image, read from the head of the object.

    The `total:i64` at the front of the container is the *uncompressed, unholed*
    size of what was checkpointed, so this answers "how big is the agent's memory
    image" from a couple of hundred kilobytes rather than by pulling the image
    down. That is the number restore work tracks; `image_bytes` from the listing
    is what the bucket is charged for. Reporting either as the other is wrong.

    Returns None rather than raising when the object is a plain zstd stream, is
    too short, or zstd cannot get a block out of the prefix. An unknown here
    costs one column in the report; a raise costs the sample.
    """
    cat = subprocess.Popen(["gcloud", "storage", "cat", uri], bufsize=0,
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        head = read_exactly(cat.stdout, prefix_bytes)
    finally:
        cat.kill()
        cat.stdout.close()
        cat.wait(timeout=10)
    if len(head) < 20 or head[:8] != SPARSE_MAGIC:
        return None
    if int.from_bytes(head[8:12], "little") != SPARSE_VERSION:
        return None
    # zstd exits non-zero on the truncated tail, having already written the
    # leading bytes we want; stdout is kept regardless of the return code.
    p = subprocess.run(["zstd", "-dc"], input=head[12:],
                       capture_output=True, timeout=timeout)
    if len(p.stdout) < 8:
        return None
    return int.from_bytes(p.stdout[:8], "little", signed=True)


def fetch_unsparse(uri: str, dest: Path, max_bytes: int, timeout: int = 900) -> dict:
    """Download a snapshot object and undo the sparse container into `dest`.

    Holes are restored by seeking, so `dest` stays sparse and a logically large
    image does not need a logically large amount of disk.
    """
    # bufsize=0 is not a tuning choice. The zstd child below is handed this pipe
    # as a file descriptor, so anything a buffered reader pulled past the 12-byte
    # header while satisfying a 12-byte read is gone: the child starts mid-frame
    # and the first int64 comes back truncated.
    cat = subprocess.Popen(["gcloud", "storage", "cat", uri], bufsize=0,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    head = read_exactly(cat.stdout, 12)
    if head[:8] != SPARSE_MAGIC:
        # A plain zstd stream. Feed back the bytes already taken off the pipe.
        dec = subprocess.Popen(["zstd", "-dc"], stdin=subprocess.PIPE,
                               stdout=dest.open("wb"), stderr=subprocess.DEVNULL)
        dec.stdin.write(head)
        for chunk in iter(lambda: cat.stdout.read(1 << 20), b""):
            dec.stdin.write(chunk)
        dec.stdin.close()
        dec.wait(timeout=timeout)
        cat.wait(timeout=10)
        return {"sparse": False, "logical_bytes": dest.stat().st_size,
                "extents": 0, "populated_bytes": dest.stat().st_size}

    version = int.from_bytes(head[8:12], "little")
    if version != SPARSE_VERSION:
        cat.kill()
        raise RuntimeError(
            f"sparse format version {version}, this reader knows {SPARSE_VERSION}. "
            "Rejecting rather than misparsing: the layout changed under us.")

    dec = subprocess.Popen(["zstd", "-dc"], stdin=cat.stdout,
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    cat.stdout.close()
    body = dec.stdout

    def i64() -> int:
        raw = read_exactly(body, 8)
        if len(raw) != 8:
            raise EOFError("truncated sparse stream: wanted an int64")
        return int.from_bytes(raw, "little", signed=True)

    extents = populated = 0
    try:
        logical = i64()
        if max_bytes and logical > max_bytes:
            raise RuntimeError(
                f"logical size {logical} is over --max-tar-bytes {max_bytes}")
        with dest.open("wb") as out:
            out.truncate(logical)
            while True:
                off = i64()
                if off == -1:
                    break
                length = i64()
                out.seek(off)
                left = length
                while left:
                    chunk = body.read(min(left, 1 << 20))
                    if not chunk:
                        raise EOFError(f"truncated extent at offset {off}")
                    out.write(chunk)
                    left -= len(chunk)
                extents += 1
                populated += length
    finally:
        body.close()
        for p in (dec, cat):
            try:
                p.kill()
            except Exception:
                pass
    return {"sparse": True, "logical_bytes": logical, "extents": extents,
            "populated_bytes": populated}


def count_skills(uri: str, max_bytes: int, timeout: int = 900) -> dict:
    """Skill count and durable-dir composition, from the tar's headers.

    The object has to come down in full: tar has no index and the last entry
    cannot be found without walking past the others. That is why this runs on its
    own, slower cadence rather than on every size sample. Only headers are
    parsed, and the temporary file is removed before returning.

    The uncompressed total here is a genuinely different number from
    `durable_bytes` in the listing, which is the stored object. Both are
    recorded. Text compresses hard, so a tenfold gap between them is normal and
    is not evidence of an error in either.
    """
    tmp = Path(tempfile.mkdtemp(prefix="growth-")) / "durable-dir.tar"
    try:
        meta = fetch_unsparse(uri, tmp, max_bytes, timeout=timeout)
        skills: set[str] = set()
        by_dir: dict[str, int] = {}
        entries = 0
        total = 0
        with tarfile.open(tmp, mode="r:") as tf:
            for m in tf:
                if not m.isfile():
                    continue
                entries += 1
                total += m.size
                parts = m.name.lstrip("./").split("/")
                if parts:
                    by_dir[parts[0]] = by_dir.get(parts[0], 0) + m.size
                # skills/<name>/... anywhere in the path. Keying on the directory
                # rather than on SKILL.md counts a skill the agent authored with a
                # different entry file, which is the case we most want to see.
                if "skills" in parts:
                    i = parts.index("skills")
                    if i + 1 < len(parts) - 1:
                        skills.add(parts[i + 1])
        return {
            "skill_count": len(skills),
            "skills": sorted(skills),
            "durable_entries": entries,
            "durable_uncompressed_bytes": total,
            "durable_top_level": by_dir,
            **meta,
        }
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)


def object_uri(uri: str, objects: list[dict], predicate) -> str:
    for o in objects:
        if predicate(o["name"]):
            return uri.rstrip("/") + "/" + o["name"]
    return ""


def durable_object_uri(uri: str, objects: list[dict]) -> str:
    return object_uri(uri, objects, lambda n: n.startswith(DURABLE_OBJECT))


def is_image_object(name: str) -> bool:
    return not name.startswith(DURABLE_OBJECT) and name != "manifest.json"


def image_object_uri(uri: str, objects: list[dict]) -> str:
    return object_uri(uri, objects, is_image_object)


def image_object_uris(uri: str, objects: list[dict]) -> list[str]:
    """EVERY image object, because rel01 writes three of them.

    A FULL snapshot on the pass 2 install holds `checkpoint.img.zstd`,
    `pages.img.zstd` and `pages_meta.img.zstd`. `image_bytes` sums the whole
    listing so it already covers all three; the logical size was read from the
    FIRST match only. On the ctrl-floor golden that is 231190 against a true
    469487, so the record carried two numbers describing different subsets of one
    snapshot and the logical one was low by half without ever looking wrong.
    """
    base = uri.rstrip("/")
    return [f"{base}/{o['name']}" for o in objects if is_image_object(o["name"])]


# --------------------------------------------------------------------------
# collector, for the cross-check only
# --------------------------------------------------------------------------

def restore_histogram(url: str) -> dict:
    """`_sum` and `_count` for successful total-phase restores, fleet-wide.

    Failed attempts carry `ate_failure_reason`/`error_type` and share the family,
    and a cancelled resume is short, so folding them in pulls the mean down. They
    are excluded here rather than subtracted later.
    """
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            body = r.read().decode("utf-8", "replace")
    except Exception as e:
        return {"error": str(e)}
    acc = {"sum": 0.0, "count": 0.0}
    for line in body.splitlines():
        if not line or line.startswith("#") or not line.startswith(RESTORE_HISTOGRAM):
            continue
        m = re.match(r"([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([0-9.eE+-]+)$", line)
        if not m:
            continue
        name, raw, value = m.group(1), m.group(2) or "", m.group(3)
        if name not in (RESTORE_HISTOGRAM + "_sum", RESTORE_HISTOGRAM + "_count"):
            continue
        labels = dict(re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', raw))
        if "ate_failure_reason" in labels or "error_type" in labels:
            continue
        phase = labels.get("ate_snapshot_phase", "total")
        if phase != "total":
            continue
        acc["sum" if name.endswith("_sum") else "count"] += float(value)
    return acc


# --------------------------------------------------------------------------
# writer
# --------------------------------------------------------------------------

class Log:
    """One append-only JSONL file, flushed per record, one lock across threads.

    Flushed rather than buffered because the two useful ways to end a soak are
    Ctrl-C and something going wrong, and a buffered writer loses the samples
    around the second one, which are the samples worth having.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = path.open("a", buffering=1)
        self._lock = threading.Lock()
        self.path = path

    def write(self, rec: dict) -> None:
        line = json.dumps(rec, sort_keys=False)
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()
            os.fsync(self._f.fileno())


# --------------------------------------------------------------------------
# the two loops
# --------------------------------------------------------------------------

class Transitions:
    """Turns a sequence of state polls into restore intervals.

    Holds, per actor, the state last observed, when it was *first* observed in
    that state, when it was *last* observed in it, and the timestamp of the poll
    before that first observation. Those four are exactly what is needed to bound
    a dwell time from below and above without pretending to a precision polling
    does not have.
    """

    def __init__(self, log: Log, interval: float):
        self.log = log
        self.interval = interval
        self.prev: dict[str, dict] = {}
        self.last_poll_t = 0.0

    def observe(self, snapshot: list[dict], t: float) -> None:
        gap = t - self.last_poll_t if self.last_poll_t else self.interval
        seen = set()
        for doc in snapshot:
            d = digest(doc)
            name, state = d["actor"], d["state"]
            seen.add(name)
            was = self.prev.get(name)
            if was is None or was["uid"] != d["uid"]:
                # First sight, or the name was recreated onto a new UID. Either
                # way there is no earlier state to measure a dwell against, and
                # inventing one would date the interval to the previous actor.
                self.prev[name] = {"state": state, "uid": d["uid"], "first_t": t,
                                   "last_t": t, "before_t": t - gap}
                continue
            if state == was["state"]:
                was["last_t"] = t
                continue
            rec = {
                "kind": "transition", "t": t,
                "actor": name, "uid": d["uid"], "worker": d["worker"],
                "from": was["state"], "to": state,
                "observed_s": round(t - was["first_t"], 3),
                "lower_s": round(max(0.0, t - was["last_t"] - gap), 3),
                "upper_s": round(t - was["before_t"], 3),
                "poll_gap_s": round(gap, 3),
                "poll_interval_s": self.interval,
            }
            if state == RUNNING and was["state"] in (RESUMING, SUSPENDED):
                rec["restore"] = True
                # SUSPENDED straight to RUNNING means the whole of RESUMING fell
                # between two polls. There is no measurement here, only a ceiling.
                rec["missed_intermediate"] = was["state"] == SUSPENDED
                if rec["missed_intermediate"]:
                    rec["observed_s"] = None
                    rec["lower_s"] = 0.0
                    rec["upper_s"] = round(gap, 3)
            self.log.write(rec)
            self.prev[name] = {"state": state, "uid": d["uid"], "first_t": t,
                               "last_t": t, "before_t": was["last_t"]}
        for gone in set(self.prev) - seen:
            self.log.write({"kind": "vanished", "t": t, "actor": gone,
                            "last_state": self.prev[gone]["state"]})
            del self.prev[gone]
        self.last_poll_t = t


def poll_states(log: Log, interval: float, stop: threading.Event) -> None:
    tr = Transitions(log, interval)
    consecutive_errors = 0
    while not stop.is_set():
        t0 = time.time()
        try:
            tr.observe(actors(), t0)
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.write({"kind": "poll_error", "t": t0, "error": str(e)[:300],
                       "consecutive": consecutive_errors})
            # Back off, but never stop: the control plane going away for a minute
            # is a finding about the run, not a reason to lose the rest of it.
            stop.wait(min(60.0, interval * consecutive_errors))
        stop.wait(max(0.0, interval - (time.time() - t0)))


def take_sizes(log: Log, metrics_url: str, skills: bool, max_bytes: int,
               note: str = "") -> int:
    """One full pass: sizes for every actor, plus the collector cross-check."""
    try:
        docs = actors()
    except Exception as e:
        log.write({"kind": "size_error", "t": time.time(), "error": str(e)[:300]})
        return 0
    n = 0
    for doc in docs:
        d = digest(doc)
        # A suspended actor has written a snapshot, so an empty URI here is the
        # instrument failing rather than an actor with nothing to measure. The
        # first version of this loop passed the empty string to list_objects,
        # got an empty list back and wrote a zero-byte record, which is how the
        # 5 to 9 Sep run produced 3546 samples of nothing without one error in
        # the log. Fail loudly and skip instead: a gap in the series is
        # recoverable, four days of confident zeroes is not.
        if not d["snapshot_uri"]:
            if d["state"] == "ACTOR_STATE_SUSPENDED":
                log.write({"kind": "size_error", "t": time.time(), **d,
                           "error": "suspended actor has no snapshot URI in "
                                    "either status.externalSnapshot or "
                                    "status.latestSnapshot; kubectl-ate is "
                                    "probably older than the control plane"})
            continue
        try:
            objects = list_objects(d["snapshot_uri"])
        except Exception as e:
            log.write({"kind": "size_error", "t": time.time(), **d,
                       "error": str(e)[:300]})
            continue
        rec = size_record(d, objects)
        # The logical image size comes off the head of the object, so it is a
        # couple of hundred kilobytes rather than a download, and it is taken on
        # every sample. It answers a different question from the stored size and
        # is the one that tracks restore work.
        imgs = image_object_uris(d["snapshot_uri"], objects)
        if imgs:
            total, unread = 0, []
            for img in imgs:
                try:
                    v = sparse_logical_bytes(img)
                except Exception as e:
                    rec["image_logical_error"] = str(e)[:200]
                    unread.append(img.rsplit("/", 1)[-1])
                    continue
                if v is None:
                    unread.append(img.rsplit("/", 1)[-1])
                else:
                    total += v
            # Name the objects that went into the number, and the ones that did
            # not. A sum over a subset is not a smaller version of the right
            # answer, it is a different measurement, and the report has to be
            # able to tell them apart after the fact.
            rec["image_logical_objects"] = [u.rsplit("/", 1)[-1] for u in imgs]
            if unread:
                rec["image_logical_unread"] = unread
            rec["image_logical_bytes"] = total if len(unread) < len(imgs) else None
        if note:
            rec["note"] = note
        log.write(rec)
        n += 1
        if skills:
            tar = durable_object_uri(d["snapshot_uri"], objects)
            if not tar:
                continue
            try:
                log.write({"kind": "skills", "t": time.time(), **d,
                           "durable_uri": tar, **count_skills(tar, max_bytes)})
            except Exception as e:
                log.write({"kind": "skills_error", "t": time.time(), **d,
                           "error": str(e)[:300]})
    if metrics_url:
        log.write({"kind": "collector", "t": time.time(),
                   "restore": restore_histogram(metrics_url)})
    return n


def sample_sizes(log: Log, interval: float, metrics_url: str, skills_every: int,
                 max_bytes: int, stop: threading.Event) -> None:
    i = 0
    while not stop.is_set():
        t0 = time.time()
        with_skills = bool(skills_every) and i % skills_every == 0
        n = take_sizes(log, metrics_url, with_skills, max_bytes)
        print(f"[{time.strftime('%H:%M:%S')}] sized {n} actors"
              f"{' + skills' if with_skills else ''}", flush=True)
        i += 1
        stop.wait(max(0.0, interval - (time.time() - t0)))


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------

def load(path: Path) -> list[dict]:
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass          # a torn last line after a kill; the rest stands
    return out


def mb(n: float) -> str:
    return f"{n / 1e6:8.1f}"


def report(path: Path) -> int:
    recs = load(path)
    if not recs:
        print(f"no records in {path}")
        return 1
    t0 = min(r["t"] for r in recs)
    span_h = (max(r["t"] for r in recs) - t0) / 3600.0
    print(f"{path}  ·  {len(recs)} records  ·  {span_h:.1f}h\n")

    sizes: dict[str, list[dict]] = {}
    for r in recs:
        if r.get("kind") == "sizes" and r.get("object_count"):
            sizes.setdefault(r["actor"], []).append(r)

    def growth(series: list[dict], field: str) -> tuple:
        """first, last and per-day rate for one field over one agent's samples."""
        s = [r for r in series if r.get(field)]
        if not s:
            return None, None, 0.0
        f_, l_ = s[0], s[-1]
        days = max((l_["t"] - f_["t"]) / 86400.0, 1e-9)
        rate = (l_[field] - f_[field]) / 1e6 / days if len(s) > 1 else 0.0
        return f_[field], l_[field], rate

    # Durable and image are reported as separate series rather than as one total.
    # A data-scope snapshot has no image at all, so a combined total alternates
    # between the two shapes and graphs the snapshot policy instead of the agent.
    print("DURABLE DIR GROWTH  (stored bytes, post-compression)")
    print(f"{'agent':16s} {'n':>4s} {'first MB':>9s} {'last MB':>9s} "
          f"{'delta MB':>9s} {'MB/day':>8s}")
    for a in sorted(sizes):
        s = sorted(sizes[a], key=lambda r: r["t"])
        f_, l_, rate = growth(s, "durable_bytes")
        if f_ is None:
            continue
        print(f"{a:16s} {len(s):4d} {mb(f_)} {mb(l_)} {mb(l_ - f_)} {rate:8.1f}")
    if not sizes:
        print("  (none: every actor sampled had no snapshot yet)")

    full = {a: [r for r in sorted(v, key=lambda r: r["t"]) if r.get("scope") == "full"]
            for a, v in sizes.items()}
    full = {a: v for a, v in full.items() if v}
    print("\nMEMORY IMAGE GROWTH  (full-scope snapshots only)")
    if not full:
        print("  (no full-scope snapshot sampled: every sample was data-only, which")
        print("   is what a fleet sampled between commits rather than between pauses")
        print("   looks like. Sample while agents are suspended, not mid-turn.)")
    else:
        print(f"{'agent':16s} {'n':>4s} {'stored first':>13s} {'stored last':>12s} "
              f"{'MB/day':>8s} {'logical first':>14s} {'logical last':>13s}")
        for a in sorted(full):
            s = full[a]
            f_, l_, rate = growth(s, "image_bytes")
            lf, ll, _ = growth(s, "image_logical_bytes")
            fmt = (lambda x: mb(x) if x else "        -")
            print(f"{a:16s} {len(s):4d} {mb(f_):>13s} {mb(l_):>12s} {rate:8.1f} "
                  f"{fmt(lf):>14s} {fmt(ll):>13s}")
        print("  stored is what the bucket holds after compression; logical is the")
        print("  uncompressed image the restore has to rebuild. Different questions.")

    # Filter on the kind as well as the flag: a `collector` record also carries a
    # `restore` key, holding the histogram dict rather than a boolean, and a
    # truthiness test alone pulls them into the per-agent series where they have
    # no `actor` at all.
    restores = [r for r in recs
                if r.get("kind") == "transition" and r.get("restore")]
    measured = [r for r in restores if not r.get("missed_intermediate")]
    bounded = [r for r in restores if r.get("missed_intermediate")]
    print(f"\nRESTORE LATENCY  ·  {len(measured)} measured, {len(bounded)} bounded only")
    if bounded and len(bounded) > len(measured):
        iv = restores[0].get("poll_interval_s")
        print(f"  MOST RESTORES WERE FASTER THAN THE {iv}s POLL. The per-agent numbers")
        print("  below are ceilings, not measurements. Re-run with a shorter --interval.")
    # Fall back to the ceilings when nothing was measured, rather than printing
    # the warning above and then an empty table. A ceiling is still a result: it
    # says restores are under the poll interval, which for a trend question is
    # most of the answer.
    source, field = (measured, "observed_s") if measured else (bounded, "upper_s")
    per: dict[str, list[float]] = {}
    for r in source:
        per.setdefault(r["actor"], []).append(r[field])
    if per:
        label = "p50" if measured else "p50<="
        print(f"{'agent':16s} {'n':>3s} {'min':>7s} {label:>7s} {'max':>7s} "
              f"{'first third':>12s} {'last third':>11s}")
        for a in sorted(per):
            v = per[a]
            third = max(1, len(v) // 3)
            print(f"{a:16s} {len(v):3d} {min(v):7.2f} {statistics.median(v):7.2f} "
                  f"{max(v):7.2f} {statistics.median(v[:third]):12.2f} "
                  f"{statistics.median(v[-third:]):11.2f}")

    coll = [r for r in recs if r.get("kind") == "collector"
            and isinstance(r.get("restore"), dict) and r["restore"].get("count")]
    if coll and measured:
        first, last = coll[0]["restore"], coll[-1]["restore"]
        dc = last["count"] - first["count"]
        ds = last["sum"] - first["sum"]
        polled = statistics.median([r["observed_s"] for r in measured])
        print(f"\nCROSS-CHECK against the control plane's own histogram")
        print(f"  collector: {dc:.0f} restores, mean {ds / dc:.2f}s" if dc else
              "  collector: no restores observed in the window")
        print(f"  polled:    {len(measured)} restores, median {polled:.2f}s")
        print("  These count different populations (the collector sees every")
        print("  restore in the cluster, including warmers). A large disagreement")
        print("  on the mean means the poller is wrong, not the platform.")

    skills = {}
    for r in recs:
        if r.get("kind") == "skills":
            skills.setdefault(r["actor"], []).append(r)
    if skills:
        print("\nDURABLE DIR CONTENTS  (uncompressed, from tar headers)")
        print(f"{'agent':16s} {'skills':>7s} {'files':>7s} {'MB':>9s}  top level")
        for a in sorted(skills):
            r = sorted(skills[a], key=lambda x: x["t"])[-1]
            top = ", ".join(f"{k} {v / 1e6:.1f}MB" for k, v in
                            sorted(r["durable_top_level"].items(),
                                   key=lambda kv: -kv[1])[:3])
            print(f"{a:16s} {r['skill_count']:7d} {r['durable_entries']:7d} "
                  f"{mb(r['durable_uncompressed_bytes'])}  {top}")

    errs = [r for r in recs if r.get("kind", "").endswith("_error")]
    if errs:
        print(f"\n{len(errs)} error{'' if len(errs) == 1 else 's'}, last 3:")
        for r in errs[-3:]:
            print(f"  {time.strftime('%m-%d %H:%M', time.localtime(r['t']))} "
                  f"{r.get('actor', '-'):14s} {r['error'][:90]}")
    return 0


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("mode", choices=("once", "sample", "report"))
    ap.add_argument("--run", default="pass2", help="run tag; names the JSONL file")
    ap.add_argument("--out", default="growth-results")
    ap.add_argument("--interval", type=float, default=2.0,
                    help="state poll seconds; the resolution of every restore "
                         "number, so lowering it narrows the bounds")
    ap.add_argument("--size-interval", type=float, default=900.0,
                    help="seconds between object listings")
    ap.add_argument("--skills-every", type=int, default=4,
                    help="read durable-dir tar headers every Nth size sample; "
                         "0 disables. This is the only step that transfers data")
    ap.add_argument("--max-tar-bytes", type=int, default=2_000_000_000,
                    help="stop walking a durable tar past this uncompressed size")
    ap.add_argument("--metrics", default="http://127.0.0.1:18889/metrics",
                    help="collector, for the fleet-wide cross-check; "
                         "empty string to skip")
    ap.add_argument("--note", default="", help="recorded on `once` samples")
    a = ap.parse_args()

    path = Path(a.out) / f"growth-{a.run}.jsonl"
    if a.mode == "report":
        if not path.exists():
            print(f"{path} does not exist")
            return 1
        return report(path)

    log = Log(path)
    log.write({"kind": "meta", "t": time.time(), "mode": a.mode, "run": a.run,
               "context": CONTEXT, "atespace": ATESPACE,
               "poll_interval_s": a.interval, "size_interval_s": a.size_interval,
               "skills_every": a.skills_every, "note": a.note})

    if a.mode == "once":
        n = take_sizes(log, a.metrics, bool(a.skills_every), a.max_tar_bytes, a.note)
        print(f"sized {n} actors -> {path}")
        return 0 if n else 1

    print(f"appending to {path}")
    print(f"  states every {a.interval}s · sizes every {a.size_interval}s · "
          f"skills every {a.skills_every or '-'} size samples")
    print("  Ctrl-C to stop; everything taken so far is already on disk.")
    stop = threading.Event()
    threads = [
        threading.Thread(target=poll_states, args=(log, a.interval, stop),
                         daemon=True),
        threading.Thread(target=sample_sizes,
                         args=(log, a.size_interval, a.metrics, a.skills_every,
                               a.max_tar_bytes, stop), daemon=True),
    ]
    for t in threads:
        t.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        stop.set()
        print("\nstopping")
        for t in threads:
            t.join(timeout=10.0)
    log.write({"kind": "meta", "t": time.time(), "mode": "stopped", "run": a.run})
    return 0


if __name__ == "__main__":
    sys.exit(main())
