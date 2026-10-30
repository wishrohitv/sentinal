from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS = Counter(
    "http_requests", "HTTP requests handled", ["method", "status_code"]
)
HTTP_ERRORS = Counter("http_errors", "HTTP responses with a 5xx status")
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
DB_POOL_SIZE = Gauge("db_pool_size", "Configured maximum SQLAlchemy pool capacity")
DB_POOL_CHECKED_OUT = Gauge("db_pool_connections_checked_out", "Currently checked-out SQLAlchemy connections")
DB_POOL_CHECKED_IN = Gauge("db_pool_connections_checked_in", "Currently idle pooled SQLAlchemy connections")
DB_POOL_CHECKOUTS = Counter("db_pool_checkouts", "Successful SQLAlchemy pool checkouts")
DB_POOL_CHECKINS = Counter("db_pool_checkins", "SQLAlchemy pool checkins")
DB_POOL_CONNECTIONS_CREATED = Counter("db_pool_connections_created", "Physical DB connections created")
DB_CONNECTION_TIMEOUTS = Counter("db_connection_timeouts", "SQLAlchemy pool checkout timeouts")

# Initialize low-cardinality series so Prometheus can distinguish zero from missing.
HTTP_REQUESTS.labels(method="GET", status_code="200")
HTTP_REQUESTS.labels(method="POST", status_code="201")
HTTP_ERRORS.inc(0)
DB_CONNECTION_TIMEOUTS.inc(0)
