CREATE TABLE "core"."parcel_id" (
	"ParcelId" varchar,
	"parcel_number" varchar
);

CREATE SEQUENCE IF NOT EXISTS core.redbID 
INCREMENT BY 1 
START 10000001
OWNED BY core.parcel_id."ParcelId";

INSERT INTO "core"."parcel_id" ("ParcelId", "parcel_number")
	SELECT "ParcelId", nextval('core.redbID')
	FROM "staging_1"."prcl_prcl";