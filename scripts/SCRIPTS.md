## Dataset Download Scripts

```bash
# Download the full week of datasets (08-11-21 to 14-11-21) into data/
./scripts/download-all-data.sh

# Download into a custom directory
./scripts/download-all-data.sh path/to/dir

# Download a single day (default: 08-11-21)
./scripts/download-data.sh
```

## Dataset Profiling

```bash
# from the repo root (defaults: data/ in, output/profile/ out)
uv run scripts/profile_dataset.py

# or with explicit directories
uv run scripts/profile_dataset.py <data_dir> <out_dir>

# from inside scripts/, the way you ran it; paths are relative to where
# you run it
uv run profile_dataset.py ../data ../profile-outputs
```

## Pipeline Scripts

```bash
# Start full pipeline with docker compose
./scripts/run-pipeline.sh

# Reset Kafka topics and restart streams
./scripts/reset-topics.sh
```
