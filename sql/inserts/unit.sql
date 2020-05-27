CREATE TABLE "core"."unittest" (
    "unit_id" varchar PRIMARY KEY,
    "building_id" varchar,
    "description" varchar,
    "condominium" boolean,
    "create_date" date,
    "current_flag" boolean,
    "removed_flag" boolean,
    "etl_job" varchar,
    "update_date" date
);

CREATE SEQUENCE IF NOT EXISTS core.unitID 
INCREMENT BY 1 
START 1001
OWNED BY core.unittest.unit_id;