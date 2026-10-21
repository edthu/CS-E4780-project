# from the repo root (defaults: data/ in, output/profile/ out)
uv run scripts/profile_dataset.py

# or with explicit directories
uv run scripts/profile_dataset.py <data_dir> <out_dir>

# from inside scripts/, the way you ran it; paths are relative to where
you run it
uv run profile_dataset.py ../data ../profile-outputs
