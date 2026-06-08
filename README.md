# GCP Dataproc Workflow Templates

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Apache%20Spark-E25A1C?logo=apachespark)
![Dataproc](https://img.shields.io/badge/GCP-Dataproc-4285F4?logo=googlecloud)
![Cloud Build](https://img.shields.io/badge/Cloud%20Build-CI%2FCD-4285F4?logo=googlecloud)
![License](https://img.shields.io/badge/License-MIT-green)

> Google Cloud Dataproc workflow templates for running managed Spark batch jobs: PySpark word-count reference implementation, Cloud Build CI/CD integration, and cluster lifecycle management.

## Architecture
```
Source Data (GCS bucket)
        ↓
Dataproc Workflow Template
  ├── Create managed cluster (auto-sized)
  ├── Submit PySpark job(s)
  │   └── wordcount.py — MapReduce word frequency
  ├── Write results to GCS output bucket
  └── Delete cluster (cost-saving)
        ↓
GCS Output (word frequencies as CSV/Parquet)
        ↓
[Optional] BigQuery load for further analysis
```

## Workflow Template Benefits
- **Ephemeral clusters** — created at job start, deleted on completion → no idle cost
- **Repeatable** — same template runs in dev, staging, and prod
- **CI/CD ready** — `cloudbuild.yaml` triggers Dataproc on every push

## Project Structure
```
├── wordcount.py          # PySpark MapReduce word-count job
├── cloudbuild.yaml       # Cloud Build CI/CD trigger
└── README.md
```

## Usage
```bash
# Submit via gcloud
gcloud dataproc workflow-templates instantiate my-workflow \
  --region=us-central1 \
  --parameters=INPUT_PATH=gs://my-bucket/input/,OUTPUT_PATH=gs://my-bucket/output/

# Or via Cloud Build (triggered on push)
gcloud builds submit --config cloudbuild.yaml
```

## Skills Demonstrated
`PySpark` · `Google Dataproc` · `Apache Spark` · `Cloud Build` · `GCS` · `Batch Processing` · `Workflow Orchestration` · `GCP`
