set dotenv-load

# Generate CLN analytics dataset from remote node
dataset:
    ./scripts/fetch-dataset.sh > node_analytics.json.xz

# Generate a snapshot and the dashboard from it
dashboard:
    cargo run -- snapshot target/snapshot
    cargo run -- dashboard target/snapshot target/site

# Serve the generated dashboard with miniserve
serve: dashboard
    miniserve --index index.html --port 3535 target/site -i 127.0.0.1
