# Generate the dashboard
dashboard:
    cargo run -- dashboard target

# Serve the generated dashboard with miniserve
serve: dashboard
    miniserve --index index.html --port 3535 target/
