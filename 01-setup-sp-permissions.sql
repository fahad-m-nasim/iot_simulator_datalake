-- Grant catalog usage
GRANT USE CATALOG ON CATALOG dev_catalog TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;

-- Grant schema permissions
GRANT USE SCHEMA ON SCHEMA dev_catalog.bronze TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;
GRANT USE SCHEMA ON SCHEMA dev_catalog.silver TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;
GRANT USE SCHEMA ON SCHEMA dev_catalog.gold TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;

-- Grant create/modify permissions
GRANT CREATE TABLE ON SCHEMA dev_catalog.bronze TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;
GRANT CREATE TABLE ON SCHEMA dev_catalog.silver TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;
GRANT CREATE TABLE ON SCHEMA dev_catalog.gold TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;

-- For reading from volumes
GRANT READ VOLUME ON VOLUME dev_catalog.landing.vol01 TO `a1126402-7cc5-4067-99be-feb57b1d2b7c`;