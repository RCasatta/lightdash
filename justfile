set dotenv-load

# Generate CLN analytics dataset from remote node
dataset:
    ./scripts/fetch-dataset.sh > node_analytics.json.xz

# Generate the dashboard
dashboard:
    cargo run -- dashboard target --min-channels 100 --availdb test-json/availdb.json

# Serve the generated dashboard with miniserve
serve: dashboard
    miniserve --index index.html --port 3535 target/ -i 127.0.0.1
