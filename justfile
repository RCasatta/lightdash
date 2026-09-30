set dotenv-load

# Generate a test-data snapshot and the dashboard from it
dashboard:
    cargo run -- --test-data snapshot target/snapshot
    cargo run -- dashboard target/snapshot target/site

# Serve the generated dashboard with miniserve
serve: dashboard
    miniserve --index index.html --port 3535 target/site -i 127.0.0.1
