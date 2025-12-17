-- Analysis catalog bootstrap: create logical schemas only.
-- Data remains in MinIO; these schemas will host VIEW/MACRO definitions.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dwd;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS ads;

