-- Grants for the application role used by the Spark consumers (spark_user).
-- Run as the schema owner / a superuser (e.g. postgres).
--
-- Why each grant:
--   USAGE on schema      -> lets the role reference objects in the schema at all
--   SELECT               -> Spark's JDBC writer probes the table (tableExists) before append
--   INSERT               -> bronze append + gold_live snapshot insert
--   TRUNCATE             -> gold_live tables are TRUNCATE'd then re-inserted each batch
--   ALTER DEFAULT PRIV.  -> future tables in the schema auto-grant, so this is one-and-done

-- ── bronze (append-only ingestion) ───────────────────────────────────────────
GRANT USAGE ON SCHEMA bronze TO spark_user;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA bronze TO spark_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT SELECT, INSERT ON TABLES TO spark_user;

-- ── gold_live (snapshot replace: TRUNCATE + INSERT each batch) ────────────────
GRANT USAGE ON SCHEMA gold_live TO spark_user;
GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA gold_live TO spark_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold_live
    GRANT SELECT, INSERT, TRUNCATE ON TABLES TO spark_user;
