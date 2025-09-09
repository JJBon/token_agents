# tools/market_tools_new/ingest_research.py
import os, io, re, json, time, argparse, traceback, string
from contextlib import contextmanager
from botocore.config import Config as BotoConfig
import boto3

# local helpers
from .tools import _hash as _h, _chunk as _chunk_fn, _norm_text, embed_texts
from vectors.s3vectors_client import S3Vectors
from .storage_glue_marketing import ensure_marketing_tables
from tools.athena_client import query  # for Iceberg stub

AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
S3V_BUCKET   = os.getenv("VEC_BUCKET")
S3V_INDEX    = os.getenv("VEC_INDEX_RESEARCH")
KB_DATA_SOURCE_ID = os.getenv("KB_DATA_SOURCE_ID")  # optional; used to detect KB-ingested vectors

BOTO_CFG = BotoConfig(
    connect_timeout=5,
    read_timeout=30,
    retries={"max_attempts": 3, "mode": "standard"},
    max_pool_connections=10,
)

@contextmanager
def step(label: str):
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] ▶ {label} ...", flush=True)
    try:
        yield
        dt = time.time() - t0
        print(f"[{time.strftime('%H:%M:%S')}] ✓ {label} ({dt:.2f}s)", flush=True)
    except Exception as e:
        dt = time.time() - t0
        print(f"[{time.strftime('%H:%M:%S')}] ✗ {label} FAILED after {dt:.2f}s: {e}", flush=True)
        raise

# ---------------- I/O ----------------
def fetch_pdf_bytes(s3_uri: str) -> bytes:
    assert s3_uri.startswith("s3://")
    _, _, path = s3_uri.partition("s3://")
    bucket, _, key = path.partition("/")
    s3 = boto3.client("s3", region_name=AWS_REGION, config=BOTO_CFG)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

# ---------------- Extractors ----------------
def extract_text_fast(pdf_bytes: bytes) -> str:
    """pdfminer; fallback to latin-1 decode."""
    try:
        from pdfminer.high_level import extract_text
        t = extract_text(io.BytesIO(pdf_bytes)) or ""
        if t.strip():
            return t
    except Exception:
        pass
    try:
        return pdf_bytes.decode("latin-1", "ignore")
    except Exception:
        return ""

def extract_text_robust(pdf_bytes: bytes) -> str:
    """Try pdfminer -> PyPDF -> minimal pdfminer again."""
    # 1) pdfminer
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        t = pdfminer_extract(io.BytesIO(pdf_bytes)) or ""
        if len(t.strip()) > 300:
            return t
    except Exception:
        pass
    # 2) PyPDF
    try:
        from pypdf import PdfReader
        r = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for p in r.pages:
            txt = (p.extract_text() or "").strip()
            if txt:
                pages.append(txt)
        t = "\n\n".join(pages)
        if len(t.strip()) > 300:
            return t
    except Exception:
        pass
    # 3) last fallback to pdfminer even if short
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        return pdfminer_extract(io.BytesIO(pdf_bytes)) or ""
    except Exception:
        return ""

# ---------------- Chunking ----------------
_JUNK_MARKERS = (" endobj ", " xref ", " obj<<", " stream ", "%%EOF")

def looks_like_text(s: str) -> bool:
    if not s or len(s) < 20:
        return False
    printable_ratio = sum(ch in string.printable for ch in s) / max(1, len(s))
    if printable_ratio < 0.9:
        return False
    ls = s.lower()
    if any(m in ls for m in _JUNK_MARKERS):
        return False
    return True

def smart_chunks(text: str, max_len: int = 1200, min_len: int = 160):
    """Sentence-ish packing, filters PDF boilerplate, min-length gate, de-dups within run."""
    text = _norm_text(text)
    if not text:
        return []
    sents = re.split(r"(?<=[.!?])\s+", text)
    cur, cur_len, out = [], 0, []
    for s in sents:
        if not looks_like_text(s):
            continue
        if cur_len + len(s) <= max_len or not cur:
            cur.append(s); cur_len += len(s)
        else:
            chunk = " ".join(cur).strip()
            if len(chunk) >= min_len:
                out.append(chunk)
            cur, cur_len = [s], len(s)
    if cur:
        chunk = " ".join(cur).strip()
        if len(chunk) >= min_len:
            out.append(chunk)
    # coarse fallback
    if not out and len(text) >= min_len:
        for i in range(0, len(text), max_len):
            piece = text[i:i+max_len].strip()
            if looks_like_text(piece) and len(piece) >= min_len:
                out.append(piece)
    # de-dup
    seen, deduped = set(), []
    for c in out:
        h = _h(c, n=8)
        if h not in seen:
            seen.add(h); deduped.append(c)
    return deduped

# ---------------- IDs ----------------
def make_doc_id_from_s3(s3_uri: str) -> str:
    """Stable identity from S3 object (bucket/key + ETag + size)."""
    assert s3_uri.startswith("s3://")
    _, _, path = s3_uri.partition("s3://")
    bucket, _, key = path.partition("/")
    s3 = boto3.client("s3", region_name=AWS_REGION, config=BOTO_CFG)
    head = s3.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag", "").strip('"')
    size = head.get("ContentLength")
    return _h(f"{bucket}/{key}:{etag}:{size}", n=16)

# ---------------- Vectors: presence & idempotence ----------------
_KEY_RE = re.compile(r"^research#(?P<doc>[^#]+)#m:[^#]+#v\d+#h:(?P<h>[0-9a-f]{16})$")

def existing_chunk_hashes(
    doc_id: str,
    *,
    bucket=S3V_BUCKET,
    index=S3V_INDEX,
    top_k: int = 30  # must be <= 30
) -> set[str]:
    s3v = S3Vectors(region=AWS_REGION, bucket=bucket, index=index)
    seed_vec = embed_texts(["probe"], region=AWS_REGION)[0]
    hits = s3v.query(seed_vec, top_k=top_k, filt={"doc_id": doc_id})  # <= 30
    hashes = set()
    for h in hits or []:
        key = (h or {}).get("key") or (h or {}).get("id") or ""
        m = _KEY_RE.match(key)
        if m and m.group("doc") == doc_id:
            hashes.add(m.group("h"))
    return hashes


def kb_vectors_exist_for_uri(s3_uri: str) -> bool:
    if not KB_DATA_SOURCE_ID:
        return False
    try:
        s3v = S3Vectors(region=AWS_REGION, bucket=S3V_BUCKET, index=S3V_INDEX)
        seed_vec = embed_texts(["probe"], region=AWS_REGION)[0]
        if s3v.query(seed_vec, 1, {"x-amz-bedrock-kb-data-source-id": KB_DATA_SOURCE_ID}):
            return True
        if s3v.query(seed_vec, 1, {"x-amz-bedrock-kb-source-uri": s3_uri}):
            return True
    except Exception:
        pass
    return False

def put_vectors(doc_id: str, parts, *, index=S3V_INDEX, bucket=S3V_BUCKET, batch_size=64,
                only_new: bool = True, source_s3: str | None = None) -> tuple[int, int]:
    """Embed & upsert; write only missing chunk hashes when only_new=True."""
    if not parts:
        return (0, 0)
    s3v = S3Vectors(region=AWS_REGION, bucket=bucket, index=index)
    already = existing_chunk_hashes(doc_id, bucket=bucket, index=index) if only_new else set()

    embs = embed_texts(list(parts), region=AWS_REGION)
    batch, n_put, n_skip = [], 0, 0

    for ord_, (p, v) in enumerate(zip(parts, embs)):
        h = _h(p, n=8)
        if h in already:
            n_skip += 1
            continue

        key = f"research#{doc_id}#m:e5-small#v1#h:{h}"
        batch.append({
                "key": key,
                "data": {"float32": [float(x) for x in v]},
                "metadata": {
                    "doc_id": doc_id,
                    "ord": int(ord_),
                    "doc_type": "research",
                    "excerpt": (p or "")[:800],
                    **({"source_s3": source_s3} if source_s3 else {}),  # <-- NEW
                },
            })
        if len(batch) >= batch_size:
            s3v.put_vectors(batch)
            n_put += len(batch)
            print(f"  • put_vectors batch={len(batch)} total_put={n_put} (skipped={n_skip})", flush=True)
            batch = []

    if batch:
        s3v.put_vectors(batch)
        n_put += len(batch)
        print(f"  • put_vectors batch={len(batch)} total_put={n_put} (skipped={n_skip})", flush=True)

    return n_put, n_skip

# ---------------- Iceberg stub ----------------
def persist_stub_to_iceberg(doc_id: str, s3_uri: str, passages: int):
    ensure_marketing_tables()
    sql = f"""
    MERGE INTO marketing.research_docs t
    USING (SELECT '{doc_id}' AS doc_id) s
      ON (t.doc_id = s.doc_id)
    WHEN MATCHED THEN UPDATE SET
        source_s3 = '{s3_uri}',
        chunks_count = {passages},
        last_indexed_at = current_timestamp
    WHEN NOT MATCHED THEN INSERT (doc_id, source_s3, chunks_count, last_indexed_at)
        VALUES ('{doc_id}', '{s3_uri}', {passages}, current_timestamp)
    """
    query(sql)
    return True

# ---------------- Textract OCR ----------------
def extract_text_textract_from_s3(s3_uri: str) -> str:
    """OCR PDF via Textract async API (S3 only). Returns concatenated text."""
    assert s3_uri.startswith("s3://")
    _, _, path = s3_uri.partition("s3://")
    bucket, _, key = path.partition("/")

    cli = boto3.client("textract", region_name=AWS_REGION, config=BOTO_CFG)
    job = cli.start_document_text_detection(DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}})
    job_id = job["JobId"]

    next_token = None
    texts = []
    while True:
        if next_token:
            resp = cli.get_document_text_detection(JobId=job_id, MaxResults=1000, NextToken=next_token)
        else:
            resp = cli.get_document_text_detection(JobId=job_id, MaxResults=1000)
        status = resp["JobStatus"]
        blocks = resp.get("Blocks", [])
        texts.extend([b["Text"] for b in blocks if b.get("BlockType") == "LINE" and b.get("Text")])
        next_token = resp.get("NextToken")

        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS") and not next_token:
            break
        time.sleep(2)

    return "\n".join(texts)

def text_quality(s: str) -> dict:
    if not s:
        return {"alpha_ratio": 0.0, "digit_ratio": 0.0, "word_count": 0}
    n = len(s)
    alpha = sum(ch.isalpha() for ch in s) / n
    digit = sum(ch.isdigit() for ch in s) / n
    words = [w for w in re.split(r"\W+", s) if w]
    return {"alpha_ratio": alpha, "digit_ratio": digit, "word_count": len(words)}

def looks_like_pdf_garbage(s: str) -> bool:
    q = text_quality(s)
    # lots of digits, few letters, and classic xref patterns
    bad_tokens = (" 65535 f", " 000000", " obj ", " endobj ", " xref ")
    if q["alpha_ratio"] < 0.25 and q["digit_ratio"] > 0.30:
        return True
    if any(tok in s for tok in bad_tokens):
        return True
    # also reject when there are < 20 words overall
    return q["word_count"] < 20

def extract_text_pymupdf4llm(pdf_bytes: bytes) -> str:
    """
    Use pymupdf4llm to extract Markdown (good layout, minimal junk).
    Works with in-memory bytes; falls back to a temp file if needed.
    """
    try:
        import pymupdf4llm  # type: ignore
        import fitz  # PyMuPDF
    except Exception:
        return ""

    # Try in-memory doc first
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            md = pymupdf4llm.to_markdown(doc) or ""
        finally:
            doc.close()
        return md
    except Exception:
        pass

    # Fallback to a temp file
    try:
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as f:
            f.write(pdf_bytes)
            f.flush()
            md = pymupdf4llm.to_markdown(f.name) or ""
            return md
    except Exception:
        return ""

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("s3_uri")
    ap.add_argument("--print-json", action="store_true")
    ap.add_argument("--persist-iceberg", action="store_true")

    # ingestion behavior
    ap.add_argument(
        "--mode",
        choices=["auto", "kb", "vectors"],
        default=os.getenv("RESEARCH_INGEST_MODE", "auto"),
        help="auto: prefer KB if present; kb: never write vectors; vectors: force write to S3 Vectors.",
    )
    ap.add_argument("--min-chars", type=int, default=160)
    ap.add_argument("--max-chars", type=int, default=1200)
    ap.add_argument("--no-ocr", action="store_true", help="Disable Textract OCR fallback.")

    # idempotence
    ap.add_argument("--skip-if-present", action="store_true",
                    help="If any vectors exist for this doc_id, skip the run entirely.")

    # single switch for write mode (default: only-new True)
    ap.add_argument("--overwrite", action="store_true",
                    help="Rewrite all chunks for this doc_id (default: only write missing chunks).")
    
    ap.add_argument(
    "--extract-mode",
    choices=["auto", "robust", "fast", "pymupdf"],
    default="auto",
    help="Extraction strategy and fallback ordering."
    )

    args = ap.parse_args()


    out = {
        "doc_id": None,
        "passages": 0,
        "chunks_indexed": 0,
        "chunks_skipped": 0,
        "mode": args.mode,
        "extract_mode": args.extract_mode,
    }

    # Stable identity before any heavy work
    doc_id = make_doc_id_from_s3(args.s3_uri)
    out["doc_id"] = doc_id

    # Preflight skip if doc already present
    if args.skip_if_present and existing_chunk_hashes(doc_id):
        print("  (skip-if-present) vectors already exist for this doc; skipping.", flush=True)
        if args.print_json: print(json.dumps(out, indent=2, ensure_ascii=False))
        return

    # If mode==kb and KB has vectors for this URI, we can bail early
    if args.mode in ("auto", "kb"):
        if kb_vectors_exist_for_uri(args.s3_uri):
            print("  KB already has vectors for this doc; Python vector write suppressed.", flush=True)
            if args.persist_iceberg:
                with step("Persist stub to Iceberg"):
                    persist_stub_to_iceberg(doc_id, args.s3_uri, 0)
            if args.print_json: print(json.dumps(out, indent=2, ensure_ascii=False))
            return

    try:
        with step("Fetch PDF from S3"):
            blob = fetch_pdf_bytes(args.s3_uri)

        # --- extract ---
        with step("Extract text"):
            text = ""
            if args.extract_mode == "pymupdf":
                text = extract_text_pymupdf4llm(blob) or ""
            elif args.extract_mode == "robust":
                text = extract_text_robust(blob) or ""
            elif args.extract_mode == "fast":
                text = extract_text_fast(blob) or ""
            else:
                # auto: try pymupdf4llm -> robust -> fast
                text = extract_text_pymupdf4llm(blob) or ""
                if len((text or "").strip()) < 300:
                    text = extract_text_robust(blob) or text
                if len((text or "").strip()) < 300:
                    text = extract_text_fast(blob) or text

            print(f"  extracted_chars={len(text)}", flush=True)
            if text:
                print(f"  preview: {_norm_text(text)[:240]!r}", flush=True)

        # --- chunk ---
        with step("Chunk text"):
            parts = smart_chunks(text, max_len=args.max_chars, min_len=args.min_chars) if text else []
            print(f"  passages={len(parts)}", flush=True)

            if args.extract_mode == "auto" and not parts:
                print("  (auto) robust produced 0 chunks → retrying fast extractor...", flush=True)
                text2 = extract_text_fast(blob)
                print(f"  extracted_chars_fast={len(text2)}", flush=True)
                parts = smart_chunks(text2, max_len=args.max_chars, min_len=args.min_chars) if text2 else []
                print(f"  passages_after_fast={len(parts)}", flush=True)
            else:
                # NEW: if parts exist but they all look like garbage, try OCR anyway
                if parts and not args.no_ocr and all(looks_like_pdf_garbage(c) for c in parts):
                    print("  Parts look like PDF garbage → forcing Textract OCR...", flush=True)
                    try:
                        text3 = extract_text_textract_from_s3(args.s3_uri)
                        print(f"  extracted_chars_textract={len(text3)}", flush=True)
                        parts2 = smart_chunks(text3, max_len=args.max_chars, min_len=args.min_chars) if text3 else []
                        print(f"  passages_after_textract={len(parts2)}", flush=True)
                        if parts2:  # replace with OCR chunks if any
                            parts = parts2
                    except Exception as e:
                        print(f"  Textract OCR forced fallback failed: {e}", flush=True)

            if not parts and not args.no_ocr:
                try:
                    print("  No usable chunks → trying Textract OCR...", flush=True)
                    text3 = extract_text_textract_from_s3(args.s3_uri)
                    print(f"  extracted_chars_textract={len(text3)}", flush=True)
                    parts = smart_chunks(text3, max_len=args.max_chars, min_len=args.min_chars) if text3 else []
                    print(f"  passages_after_textract={len(parts)}", flush=True)
                except Exception as e:
                    print(f"  Textract OCR fallback failed: {e}", flush=True)

        out["passages"] = len(parts)

        # Decide whether we will write vectors from Python
        should_write_vectors = (args.mode == "vectors") or (args.mode == "auto")
        if args.mode == "vectors":
            should_write_vectors = True
        if args.mode == "kb":
            should_write_vectors = False
        else:  # auto
            should_write_vectors = not (
                KB_DATA_SOURCE_ID and kb_vectors_exist_for_uri(args.s3_uri)
            )

        if should_write_vectors and parts:
            with step("Embed + put_vectors"):
                only_new= not args.overwrite
                n_put, n_skip = put_vectors(
                    doc_id,
                    parts,
                    only_new=only_new,
                    source_s3=args.s3_uri,
                )
            out["chunks_indexed"] = n_put
            out["chunks_skipped"] = n_skip
        else:
            print("  Skipped vector write.", flush=True)

        if args.persist_iceberg:
            with step("Persist stub to Iceberg"):
                persist_stub_to_iceberg(doc_id, args.s3_uri, out["passages"])

        if args.print_json:
            print(json.dumps(out, indent=2, ensure_ascii=False))
        else:
            print(f"[DONE] {out}", flush=True)

    except Exception:
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
