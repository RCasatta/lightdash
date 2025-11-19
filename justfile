# Generate the dashboard
dashboard:
    cargo run -- dashboard target --min-channels 100 --availdb test-json/availdb.json

# Serve the generated dashboard with miniserve
serve: dashboard
    miniserve --index index.html --port 3535 target/ -i 127.0.0.1
