-- Run each query separately in DBeaver after loading the data.

-- 1. Basic pipeline result: accepted data by HTTP status code.
SELECT status_code, COUNT(*) AS request_count
FROM web_requests
GROUP BY status_code
ORDER BY request_count DESC;

-- 2. Endpoint traffic and HTTP error rate. This treats both 4xx and 5xx as errors.
SELECT url_path,
       COUNT(*) AS total_requests,
       COUNT(*) FILTER (WHERE status_code >= 400) AS error_requests,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status_code >= 400)
             / NULLIF(COUNT(*), 0), 2) AS error_rate_pct
FROM web_requests
GROUP BY url_path
HAVING COUNT(*) >= 5
ORDER BY error_rate_pct DESC, total_requests DESC
LIMIT 25;

-- 3. Hourly traffic pattern (stored timestamps are UTC).
SELECT date_trunc('hour', event_timestamp) AS hour_utc,
       COUNT(*) AS request_count,
       SUM(COALESCE(bytes_sent, 0)) AS bytes_transferred
FROM web_requests
GROUP BY hour_utc
ORDER BY hour_utc;

-- 4. Top requested resources.
SELECT url_path,
       COUNT(*) AS request_count,
       ROUND(AVG(bytes_sent)::numeric, 1) AS average_bytes_sent
FROM web_requests
GROUP BY url_path
ORDER BY request_count DESC
LIMIT 20;

-- 5. Daily traffic, errors, and transfer volume.
SELECT event_timestamp::date AS date_utc,
       COUNT(*) AS requests,
       COUNT(*) FILTER (WHERE status_code >= 400) AS error_requests,
       SUM(COALESCE(bytes_sent, 0)) AS bytes_transferred
FROM web_requests
GROUP BY date_utc
ORDER BY date_utc;

-- 6. Inspect a small sample of errors. Avoid publishing or profiling individual hosts.
SELECT event_timestamp, http_method, url_path, status_code, bytes_sent
FROM web_requests
WHERE status_code >= 400
ORDER BY event_timestamp
LIMIT 50;
