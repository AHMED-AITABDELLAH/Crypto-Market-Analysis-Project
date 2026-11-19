-- Postgres LOCAL DE TEST uniquement (profil "dev").
-- La vraie base est geree par le Database / ETL Engineer.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
