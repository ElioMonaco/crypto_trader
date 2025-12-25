CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
  'recluster-market_events',
  '0 3 * * * SUN', 
  'CLUSTER market_events USING idx_exchange_timestamp;'
);