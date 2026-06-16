"""
TC-07 — Procurement document refresh DAG.

Simulates a daily ETL for the RAG knowledge base (TC-03):
  detect new procurement docs → validate (size + format in parallel)
  → build indexing manifest → notify

Designed for Airflow 2.10.5 on AIE 1.9.1. Uses only the standard
TaskFlow API — no external operators, no extra connections.

Place in the Airflow DAGs folder (typically /opt/airflow/dags on the
scheduler+webserver pods). The DAG will be picked up within ~30 seconds
and appear in the Airflow UI under tag 'aie'.
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task


DEFAULT_ARGS = {
    "owner": "daniel",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="procurement_doc_refresh",
    description="Refresh procurement PDFs into the RAG knowledge base",
    start_date=datetime(2026, 6, 1),
    schedule=None,                    # manual trigger for the demo
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["aie", "rag", "demo"],
    params={
        "source_path": "/data/procurement/inbox",
        "min_pages": 1,
        "max_pages": 500,
    },
)
def procurement_doc_refresh():

    @task
    def check_source(**context) -> list[str]:
        """List candidate PDFs from the source folder."""
        path = context["params"]["source_path"]
        # In a real DAG: glob the PVC. Here we simulate.
        files = ["procurement_act_2017.pdf",
                 "amendment_2024.pdf",
                 "guideline_v2.pdf"]
        print(f"Source: {path}")
        print(f"Found {len(files)} candidate document(s):")
        for f in files:
            print(f"  - {f}")
        return files

    @task
    def validate_size(files: list[str], **context) -> list[dict]:
        """Reject documents outside the configured page range."""
        import random
        min_p = context["params"]["min_pages"]
        max_p = context["params"]["max_pages"]
        results = []
        for f in files:
            pages = random.randint(50, 250)
            ok = min_p <= pages <= max_p
            results.append({"file": f, "pages": pages, "ok": ok})
            print(f"  {f}: {pages} pages — {'OK' if ok else 'REJECTED'}")
        return results

    @task
    def validate_format(files: list[str]) -> list[dict]:
        """Confirm format is PDF (placeholder for real MIME inspection)."""
        results = [{"file": f, "format": "application/pdf", "ok": True}
                   for f in files]
        for r in results:
            print(f"  {r['file']}: format={r['format']} — OK")
        return results

    @task
    def build_manifest(size_results: list[dict],
                       format_results: list[dict]) -> dict:
        """Produce an indexing manifest for the downstream RAG ingest."""
        accepted = [r for r in size_results if r["ok"]]
        manifest = {
            "timestamp": datetime.utcnow().isoformat(),
            "files_accepted": len(accepted),
            "files_rejected": len(size_results) - len(accepted),
            "total_pages": sum(r["pages"] for r in accepted),
            "status": "ready_to_index",
            "files": [r["file"] for r in accepted],
        }
        print("MANIFEST:")
        for k, v in manifest.items():
            print(f"  {k}: {v}")
        return manifest

    @task
    def notify(manifest: dict) -> str:
        """Log notification of successful refresh."""
        msg = (
            f"[{manifest['timestamp']}] "
            f"RAG refresh complete: {manifest['files_accepted']} doc(s) / "
            f"{manifest['total_pages']} pages ready to index. "
            f"({manifest['files_rejected']} rejected.)"
        )
        print(msg)
        # In real life: post to Slack, email, or trigger OWU re-ingest API.
        return msg

    # Task graph — parallel validation branches join at the manifest step
    files    = check_source()
    sizes    = validate_size(files)
    formats  = validate_format(files)
    manifest = build_manifest(sizes, formats)
    notify(manifest)


procurement_doc_refresh()
