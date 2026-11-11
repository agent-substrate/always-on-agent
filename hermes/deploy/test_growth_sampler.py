"""Exercise the parts of growth_sampler that must be right before the soak starts.

Run it from anywhere:  python3 deploy/test_growth_sampler.py

Nothing here touches a cluster or a bucket. The ATESPRSE cases build their own
containers with a local writer, so a change to the on-disk format shows up as a
failure here rather than as a skill count of zero in the middle of a soak.
"""
import importlib.util, json, os, sys, tarfile, io, time, subprocess, tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("gs", _HERE / "growth_sampler.py")
gs = importlib.util.module_from_spec(spec); spec.loader.exec_module(gs)

fails = []
def check(label, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + label + (f"   {extra}" if extra and not cond else ""))
    if not cond: fails.append(label)

def doc(name, state, uid="u1", uri="gs://b/p", worker="w1"):
    return {"metadata": {"name": name, "uid": uid},
            "status": {"state": state, "workerAssignment": {"workerPod": worker},
                       "externalSnapshot": {"snapshotUri": uri}}}

# ---- digest / size_record ----
d = gs.digest(doc("a1", gs.RUNNING))
check("digest pulls name/uid/state/worker/uri",
      d == {"actor":"a1","uid":"u1","state":gs.RUNNING,"worker":"w1","snapshot_uri":"gs://b/p"}, d)

objs = [{"name":"checkpoint.img.zstd","bytes":44_000_000},
        {"name":"durable-dir.tar.zstd","bytes":3_000_000},
        {"name":"manifest.json","bytes":200}]
r = gs.size_record(d, objs)
check("durable split off the total", r["durable_bytes"] == 3_000_000, r["durable_bytes"])
check("manifest is not counted as image", r["image_bytes"] == 44_000_000, r["image_bytes"])
check("manifest kept on its own line", r["manifest_bytes"] == 200)
check("total is the sum", r["total_bytes"] == 47_000_200)
check("an image present means full scope", r["scope"] == "full")

# a DATA-scope snapshot: durable only, no memory image. This is the live shape
# seen on durdir-sqlite and it must not read as "the image shrank to a manifest".
r2 = gs.size_record(d, [{"name":"durable-dir.tar.zstd","bytes":3_000_000},
                        {"name":"manifest.json","bytes":200}])
check("DATA-scope snapshot has exactly no image", r2["image_bytes"] == 0)
check("DATA-scope snapshot is labelled data", r2["scope"] == "data")

# no snapshot yet
r3 = gs.size_record(d, [])
check("no snapshot yet is zeros not a crash",
      (r3["total_bytes"], r3["object_count"]) == (0, 0))
check("no objects is not full scope", r3["scope"] == "data")

# ---- Transitions ----
class FakeLog:
    def __init__(self): self.recs = []
    def write(self, r): self.recs.append(r)

def run(seq, interval=2.0):
    """seq is [(t, [(name,state)...])]"""
    log = FakeLog(); tr = gs.Transitions(log, interval)
    for t, states in seq:
        tr.observe([doc(n, s) for n, s in states], t)
    return log.recs

# measured restore: RESUMING seen for two polls
recs = run([(100.0, [("a1", gs.SUSPENDED)]),
            (102.0, [("a1", gs.SUSPENDED)]),
            (104.0, [("a1", gs.RESUMING)]),
            (106.0, [("a1", gs.RESUMING)]),
            (108.0, [("a1", gs.RUNNING)])])
rest = [r for r in recs if r.get("restore")]
check("one restore emitted", len(rest) == 1, [r for r in recs])
r = rest[0]
check("restore is measured, not bounded", r["missed_intermediate"] is False)
check("observed_s = first RESUMING to RUNNING", r["observed_s"] == 4.0, r["observed_s"])
check("lower_s = last RESUMING to RUNNING minus a gap", r["lower_s"] == 0.0, r["lower_s"])
check("upper_s spans the poll before RESUMING", r["upper_s"] == 6.0, r["upper_s"])
check("bounds bracket the estimate", r["lower_s"] <= r["observed_s"] <= r["upper_s"])
check("from/to recorded", (r["from"], r["to"]) == (gs.RESUMING, gs.RUNNING))

# restore faster than the poll: RESUMING never seen
recs = run([(100.0, [("a1", gs.SUSPENDED)]),
            (102.0, [("a1", gs.SUSPENDED)]),
            (104.0, [("a1", gs.RUNNING)])])
r = [x for x in recs if x.get("restore")][0]
check("sub-interval restore flagged bounded", r["missed_intermediate"] is True)
check("bounded restore has no point estimate", r["observed_s"] is None)
check("bounded restore ceiling is one poll gap", r["upper_s"] == 2.0, r["upper_s"])
check("bounded restore floor is zero", r["lower_s"] == 0.0)

# suspend is a transition but NOT a restore
recs = run([(100.0, [("a1", gs.RUNNING)]),
            (102.0, [("a1", "ACTOR_STATE_SUSPENDING")]),
            (104.0, [("a1", gs.SUSPENDED)])])
check("suspends are not counted as restores",
      not any(x.get("restore") for x in recs))
check("suspends are still recorded", len([x for x in recs if x["kind"]=="transition"]) == 2)

# a recreated actor must not inherit the old one's dwell
log = FakeLog(); tr = gs.Transitions(log, 2.0)
tr.observe([doc("a1", gs.SUSPENDED, uid="old")], 100.0)
tr.observe([doc("a1", gs.RUNNING,   uid="new")], 102.0)
check("same name, new UID is not a restore",
      not any(x.get("restore") for x in log.recs), log.recs)

# a stall in the poll loop widens the ceiling instead of lying
recs = run([(100.0, [("a1", gs.SUSPENDED)]),
            (400.0, [("a1", gs.RUNNING)])])
r = [x for x in recs if x.get("restore")][0]
check("a 300s poll gap is reported as such", r["poll_gap_s"] == 300.0)
check("a stalled poll gives a 300s ceiling, not a 300s measurement",
      r["observed_s"] is None and r["upper_s"] == 300.0)

# an actor disappearing is recorded, not treated as a transition
log = FakeLog(); tr = gs.Transitions(log, 2.0)
tr.observe([doc("a1", gs.RUNNING)], 100.0)
tr.observe([], 102.0)
check("a deleted actor is recorded as vanished",
      [x["kind"] for x in log.recs] == ["vanished"], log.recs)

# ---- list_objects parsing against the REAL gcloud JSON shape ----
real = json.dumps([{
    "url": "gs://b/p/checkpoint.img.zstd#1785191731216558",
    "type": "cloud_object",
    "metadata": {"name": "hermes/uid/snap/checkpoint.img.zstd", "size": "44539981"}},
    {"url": "gs://b/p/", "type": "prefix", "metadata": {}}])
import unittest.mock as mock
with mock.patch.object(gs.subprocess, "run",
        return_value=subprocess.CompletedProcess([], 0, real, "")):
    got = gs.list_objects("gs://b/p")
check("gcloud size string is parsed as an int",
      got == [{"name": "checkpoint.img.zstd", "bytes": 44539981}], got)
check("prefixes in the listing are skipped", len(got) == 1)
with mock.patch.object(gs.subprocess, "run",
        return_value=subprocess.CompletedProcess([], 1, "", "matched no objects")):
    check("an empty prefix is [] not an exception", gs.list_objects("gs://b/p") == [])
with mock.patch.object(gs.subprocess, "run",
        return_value=subprocess.CompletedProcess([], 1, "", "403 permission denied")):
    try:
        gs.list_objects("gs://b/p"); check("a real gcloud failure raises", False)
    except RuntimeError as e:
        check("a real gcloud failure raises", "403" in str(e))
check("no snapshot uri short-circuits without calling gcloud", gs.list_objects("") == [])

# ---- durable_object_uri ----
check("durable object uri is built from the listing",
      gs.durable_object_uri("gs://b/p/", objs) == "gs://b/p/durable-dir.tar.zstd")
check("no durable object gives empty", gs.durable_object_uri("gs://b/p", [objs[0]]) == "")
check("image object uri skips durable and manifest",
      gs.image_object_uri("gs://b/p", objs) == "gs://b/p/checkpoint.img.zstd")
check("no image object gives empty",
      gs.image_object_uri("gs://b/p", objs[1:]) == "")

# rel01 splits the memory image over checkpoint.img.zstd AND pages.img.zstd, so
# reading only the first match under-reports the logical size against an
# image_bytes that already covers both.
split = [{"name": "checkpoint.img.zstd", "bytes": 10},
         {"name": "pages.img.zstd", "bytes": 20},
         {"name": "durable-dir.tar.zstd", "bytes": 30},
         {"name": "manifest.json", "bytes": 1}]
check("every image object is returned, durable and manifest are not",
      gs.image_object_uris("gs://b/p/", split)
      == ["gs://b/p/checkpoint.img.zstd", "gs://b/p/pages.img.zstd"],
      gs.image_object_uris("gs://b/p/", split))
check("a data-only snapshot has no image objects",
      gs.image_object_uris("gs://b/p", split[2:]) == [])
check("image_bytes covers the same objects image_object_uris names",
      gs.size_record({}, split)["image_bytes"] == 30)

# ---- the ATESPRSE sparse container, round-tripped ----
# The live object on durdir-sqlite is not a zstd stream: it carries a 12-byte
# header ahead of one. Building the container here rather than trusting a
# fixture, so the test fails if the reader drifts from the writer's layout.
import struct
def sparse_container(payload: bytes, logical: int, extents):
    body = struct.pack("<q", logical)
    for off, data in extents:
        body += struct.pack("<qq", off, len(data)) + data
    body += struct.pack("<q", -1)
    z = subprocess.run(["zstd", "-q", "-c"], input=body, capture_output=True, check=True)
    return b"ATESPRSE" + struct.pack("<I", 2) + z.stdout

tmp = tempfile.mkdtemp()
tarp = os.path.join(tmp, "durable-dir.tar")
with tarfile.open(tarp, "w") as tf:
    def add(name, size):
        info = tarfile.TarInfo(name); info.size = size
        tf.addfile(info, io.BytesIO(b"x" * size))
    add("hermes-home/skills/example-tool-skill/SKILL.md", 1000)
    add("hermes-home/skills/self-authored-one/SKILL.md", 500)
    add("hermes-home/skills/self-authored-one/helper.py", 300)
    add("hermes-home/skills/no-skill-md/notes.txt", 100)   # counts: dir is the skill
    add("hermes-home/.hermes/state.db", 8000)
    add("hermes-home/memories/a.md", 200)
raw = open(tarp, "rb").read()

# One extent at 0: what a tar (a dense file) actually produces.
dense = os.path.join(tmp, "dense.bin")
open(dense, "wb").write(sparse_container(raw, len(raw), [(0, raw)]))
# Two extents with a hole between them: what a guest memory image produces.
holed = os.path.join(tmp, "holed.bin")
open(holed, "wb").write(
    sparse_container(b"", 1 << 20, [(0, b"A" * 4096), (1 << 19, b"B" * 4096)]))
# A plain zstd stream with no magic at all, which the writer is allowed to emit.
plainp = os.path.join(tmp, "plain.bin")
subprocess.run(["zstd", "-q", "-f", tarp, "-o", plainp], check=True)
# A container claiming a version this reader does not know.
badver = os.path.join(tmp, "badver.bin")
c = bytearray(open(dense, "rb").read()); c[8:12] = struct.pack("<I", 99)
open(badver, "wb").write(bytes(c))

orig = gs.subprocess.Popen
def cat_from(path):
    def fake(cmd, **kw):
        if cmd[:3] == ["gcloud", "storage", "cat"]:
            return orig(["cat", path], **kw)
        return orig(cmd, **kw)
    return fake

def with_cat(path, fn):
    gs.subprocess.Popen = cat_from(path)
    try:
        return fn()
    finally:
        gs.subprocess.Popen = orig

out = Path(tmp) / "out.bin"
m = with_cat(dense, lambda: gs.fetch_unsparse("gs://b/o", out, 0))
check("dense container round-trips byte for byte", out.read_bytes() == raw)
check("dense container reports one extent", (m["sparse"], m["extents"]) == (True, 1), m)
check("logical size is the tar's size", m["logical_bytes"] == len(raw), m)

m = with_cat(holed, lambda: gs.fetch_unsparse("gs://b/o", out, 0))
b = out.read_bytes()
check("holed container restores the logical size", len(b) == 1 << 20, len(b))
check("holed container places both extents",
      b[:4096] == b"A"*4096 and b[1<<19:(1<<19)+4096] == b"B"*4096)
check("the hole really is zeros", set(b[4096:1<<19]) == {0})
check("populated bytes exclude the hole", m["populated_bytes"] == 8192, m)
check("a 1MB logical image travelled as 8KB of extents",
      m["logical_bytes"] == 1 << 20 and m["populated_bytes"] == 8192)

m = with_cat(plainp, lambda: gs.fetch_unsparse("gs://b/o", out, 0))
check("a plain zstd stream is passed through", out.read_bytes() == raw)
check("a plain zstd stream is labelled not sparse", m["sparse"] is False)

try:
    with_cat(badver, lambda: gs.fetch_unsparse("gs://b/o", out, 0))
    check("an unknown format version is refused", False)
except RuntimeError as e:
    check("an unknown format version is refused", "version 99" in str(e), str(e))

try:
    with_cat(dense, lambda: gs.fetch_unsparse("gs://b/o", out, 100))
    check("the byte cap refuses an oversized image", False)
except RuntimeError as e:
    check("the byte cap refuses an oversized image", "max-tar-bytes" in str(e), str(e))

# ---- sparse_logical_bytes, the cheap head read ----
big = os.path.join(tmp, "big.bin")
open(big, "wb").write(sparse_container(b"", 2 << 30, [(0, b"C" * 200000)]))
n = with_cat(big, lambda: gs.sparse_logical_bytes("gs://b/o"))
check("logical size read from the head of a 2GiB image", n == 2 << 30, n)
n = with_cat(plainp, lambda: gs.sparse_logical_bytes("gs://b/o"))
check("a plain zstd stream has no logical size to read", n is None, n)
n = with_cat(badver, lambda: gs.sparse_logical_bytes("gs://b/o"))
check("an unknown version reads as unknown, not as a wrong number", n is None, n)

# ---- count_skills over the real container ----
s = with_cat(dense, lambda: gs.count_skills("gs://b/p/durable-dir.tar.zstd", 0))
check("skills counted by directory, not by SKILL.md", s["skill_count"] == 3, s["skills"])
check("skill names captured",
      s["skills"] == ["example-tool-skill", "no-skill-md", "self-authored-one"], s["skills"])
check("every file counted", s["durable_entries"] == 6, s["durable_entries"])
check("uncompressed total is the tar members', not the object's",
      s["durable_uncompressed_bytes"] == 10100, s["durable_uncompressed_bytes"])
check("top level grouped", s["durable_top_level"] == {"hermes-home": 10100},
      s["durable_top_level"])
check("count_skills leaves no temp dir behind",
      not any(p.startswith("growth-") for p in os.listdir(tempfile.gettempdir())))

# ---- restore_histogram label handling ----
body = """# TYPE ate_actor_restore_duration_seconds histogram
ate_actor_restore_duration_seconds_sum{ate_snapshot_phase="total"} 12.0
ate_actor_restore_duration_seconds_count{ate_snapshot_phase="total"} 4
ate_actor_restore_duration_seconds_sum{ate_snapshot_phase="pull"} 3.0
ate_actor_restore_duration_seconds_count{ate_snapshot_phase="pull"} 4
ate_actor_restore_duration_seconds_sum{ate_snapshot_phase="total",error_type="Canceled"} 0.5
ate_actor_restore_duration_seconds_count{ate_snapshot_phase="total",error_type="Canceled"} 9
"""
class FakeResp:
    def read(self): return body.encode()
    def __enter__(self): return self
    def __exit__(self, *a): return False
with mock.patch.object(gs.urllib.request, "urlopen", return_value=FakeResp()):
    h = gs.restore_histogram("http://x/metrics")
check("stage observations excluded from the total", h["count"] == 4.0, h)
check("cancelled restores excluded", h["sum"] == 12.0, h)
with mock.patch.object(gs.urllib.request, "urlopen", side_effect=OSError("refused")):
    check("collector down is reported, not fatal",
          "error" in gs.restore_histogram("http://x/metrics"))

print()
print(("ALL PASS" if not fails else f"{len(fails)} FAILED: {fails}"))
sys.exit(1 if fails else 0)
