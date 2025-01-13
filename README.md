# hertta

This Rust project provides a [GraphQL](https://graphql.org/) server for the Hertta system, offering an API for optimization, electricity price fetching, and weather forecast jobs.

## 1. Install Rust and Cargo

Make sure you have Rust tools installed (at least Rust 1.60+). Verify by running:

```
rustc --version
cargo --version
```

## 2. Clone or download the project
Navigate to the desired directory and clone this repository:

```
git clone https://github.com/predicer-tools/hertta.git
cd hertta
```

## 3. Build the project
In the project root, run:
```
cargo build --release
```
The application reads configuration primarily from environment variables. Additionally, the application can use a settings.toml file.

## 4. Generate the settings file

The application can automatically generate a default settings file for you:
```
cargo run -- --write-settings
```
This command:
- Creates any missing directories for the settings file path.
- Writes default settings in .toml format.
- After running the command, check the console output to see where the file was created. You can then edit the file as needed.

## 5. Create Python environment

The application needs a Python environment to fetch weather forecasts.
The required Python packages are listed at the top of [this file](https://github.com/predicer-tools/hertta/blob/a9ca8e5f584aba733fbb496b26943c805d065492/forecasts/weather_forecast.py#L2).

## 6. Run the Server

Without any additional options the application runs as a normal server:
```
cargo run
```
Once started, the server exposes a GraphQL endpoint at http://127.0.0.1:3030/graphql.

All GraphQL queries and mutations should be sent as POST requests to this endpoint.
Because the CORS policy allows any origin, you can safely query the API from your local or remote front-end application.

## 7. Use the GraphQL API

To view the GraphQL schema (in SDL format) to see what queries and mutations are available, use:
```
cargo run -- --print-schema
```

When the server is running, the GraphQL endpoint is accessible at:
```
http://127.0.0.1:3030/graphql
```
You can perform queries and mutations in multiple ways:

- Use any third-party GraphQL tool pointing to 127.0.0.1:3030/graphql
- Command line (curl)
```
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"{ settings { location { country } } }\"}" \
  http://127.0.0.1:3030/graphql
```
### Example Query

This returns the server’s location-based settings (e.g., country, place):
```
query {
  settings {
    location {
      country
      place
    }
  }
}
```
### Example Mutation

This starts an optimization job and returns its job ID (for instance, 1). You can then check its status with the jobStatus(jobId: Int!) query.
```
mutation {
  startOptimization
}
```
### Running Jobs

The application supports different workflow “jobs,” including:

- Electricity Price Fetch
- Optimization
- Weather Forecast

When a job is started via a mutation (e.g., startOptimization), it is placed in a JobStore. A Tokio-based event_loop processes them asynchronously. Each job transitions through states (QUEUED, IN_PROGRESS, FAILED, FINISHED) and you can track its status through the GraphQL API, for example:
```
query {
  jobStatus(jobId: 1) {
    state
    message
  }
}
```
When a job completes, you can retrieve its result with jobOutcome(jobId: Int!), which returns data such as electricity price forecasts, optimization control signals, or weather forecast data.
