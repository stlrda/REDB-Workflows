CREATE TABLE "parcel_history" (
  "parcel_id" int PRIMARY KEY,
  "owner_id" int,
  "municipality_id" int,
  "zoning_class_id" int,
  "address_id" int,
  "legal_desc" varchar,
  "ward" int,
  "neighborhood" varchar,
  "zip_code" int,
  "census_block" int,
  "frontage" int,
  "asr_class_code" varchar,
  "asr_land_use1" varchar,
  "asr_land_use2" varchar,
  "lead_agcy" varchar,
  "sale_price" float,
  "sale_date" date,
  "nbr_sewer_lines" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "building_history" (
  "building_id" int PRIMARY KEY,
  "parcel_id" int,
  "owner_id" int,
  "use_type_id" int,
  "legal_desc" varchar,
  "sq_footage" int,
  "year_built" int,
  "ground_floor_area" float,
  "ext_wall_type_ID" int,
  "stories" varchar,
  "sonst_type" varchar,
  "bmst_type" varchar,
  "bmst_finish_type" varchar,
  "bmst_area_finished" varchar,
  "bmst_area_partial" varchar,
  "bmst_area_total" varchar,
  "bmst_area_at_grade" varchar,
  "living_area_partial" varchar,
  "living_area_total" varchar,
  "living_area_at_grade" varchar,
  "living_area_up_down" varchar,
  "room_area" varchar,
  "framing_type" varchar,
  "story_height" varchar,
  "foundation_type" varchar,
  "condominium" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "unit_history" (
  "unit_id" int PRIMARY KEY,
  "building_id" int,
  "owner_id" int,
  "use_type_id" int,
  "address_id" int,
  "legal_desc" varchar,
  "windows_ac" boolean,
  "central_ac" boolean,
  "full_bath" int,
  "half_bath" int,
  "garage" boolean,
  "carport" boolean,
  "central_heating" boolean,
  "attic" boolean,
  "create_date" date,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "legal_entity_history" (
  "legal_entity_id" int PRIMARY KEY,
  "name" varchar,
  "address_id" int,
  "create_date" date,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "address_history" (
  "address_id" int PRIMARY KEY,
  "street_number" int,
  "street_name_prefix" varchar,
  "street_name" varchar,
  "street_name_suffix" varchar,
  "secondary_designator" varchar,
  "city" varchar,
  "county" varchar,
  "state" varchar,
  "zip" int,
  "country" varchar,
  "create_date" date,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "permit_history" (
  "permit_id" int PRIMARY KEY,
  "permit_id_type" int,
  "applicant_id" int,
  "contractor_id" int,
  "application_date" date,
  "issue_date" date,
  "completion_date" date,
  "cancel_date" date,
  "description" varchar,
  "est_project_cost" float,
  "occupant_id" int,
  "app_source_id" int,
  "cancel_type_id" int,
  "parcel_id" int,
  "new_use" varchar,
  "old_use" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "service_history" (
  "service_id" int PRIMARY KEY,
  "parcel_id" int,
  "building_id" int,
  "unit_id" int,
  "owner_id" int,
  "division_id" int,
  "service_type_id" int,
  "fee_id" int,
  "service_date" date,
  "notification_id" int,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "condemnation_history" (
  "condemnation_id" int PRIMARY KEY,
  "inspection_id" int,
  "notification_id" date,
  "condemnation_type" varchar,
  "status" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "inspection_history" (
  "inspection_id" int PRIMARY KEY,
  "building_id" int,
  "inspection_date" date,
  "inspection_type_id" int,
  "number_of_violations" int,
  "date_entered" date,
  "notification_id" int,
  "original_reinspect_date" date,
  "misc_reinspect_date" date,
  "date_abated" date,
  "date_complied" date,
  "original_inspector_number" int,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "assessment_history" (
  "assessment_id" int PRIMARY KEY,
  "parcel_id" int,
  "assessment_date" date,
  "assessment_amount" float,
  "land_value" float,
  "improvement_value" float,
  "bill_land" float,
  "bill_impr" float,
  "apr_land" float,
  "cost_apr_impove" float,
  "asmt_appeal_year" int,
  "asmt_appeal_num" int,
  "asmt_appeal_type" varchar,
  "prior_asd_land" float,
  "prior_asd_improve" float,
  "prior_asd_total" float,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "tax_delinquency_history" (
  "tax_delinquency_id" int PRIMARY KEY,
  "legal_entity_id" int,
  "last_paid_year" int,
  "amount_delinquent" float,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "fee_history" (
  "fee_id" int,
  "fee_debit_ammount" float8,
  "fee_credit_ammount" float8,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "notification_history" (
  "notification_id" int PRIMARY KEY,
  "notification_date" date,
  "notification_type" varchar,
  "parcel_id" int,
  "notification_source" varchar,
  "manage_id" int,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "municipality_history" (
  "municipality_id" int PRIMARY KEY,
  "municipality" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "division_history" (
  "division_id" int PRIMARY KEY,
  "division" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "service_type_history" (
  "service_type_id" int PRIMARY KEY,
  "service_type" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "zoning_class_history" (
  "zoning_class_id" int PRIMARY KEY,
  "zoning_class" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "municipal_parcel_id_mapping_history" (
  "parcel_id" int PRIMARY KEY,
  "municpal_parcel_id" varchar,
  "municipality_id" int,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "use_type_history" (
  "use_type_id" int PRIMARY KEY,
  "use_type" int,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "permit_type_history" (
  "permit_type_id" int PRIMARY KEY,
  "permit_type" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

CREATE TABLE "inspection_type_history" (
  "inspection_type_id" int PRIMARY KEY,
  "inspection_type" varchar,
  "create_date" date,
  "current_flag" boolean NOT NULL,
  "removed_flag" boolean NOT NULL,
  "etl_job" varchar,
  "update_date" date
);

ALTER TABLE "parcel_history" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "parcel_history" ADD FOREIGN KEY ("municipality_id") REFERENCES "municipality_history" ("municipality_id");

ALTER TABLE "parcel_history" ADD FOREIGN KEY ("zoning_class_id") REFERENCES "zoning_class_history" ("zoning_class_id");

ALTER TABLE "parcel_history" ADD FOREIGN KEY ("address_id") REFERENCES "address_history" ("address_id");

ALTER TABLE "building_history" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel_history" ("parcel_id");

ALTER TABLE "building_history" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "building_history" ADD FOREIGN KEY ("use_type_id") REFERENCES "use_type_history" ("use_type_id");

ALTER TABLE "unit_history" ADD FOREIGN KEY ("building_id") REFERENCES "building_history" ("building_id");

ALTER TABLE "unit_history" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "unit_history" ADD FOREIGN KEY ("use_type_id") REFERENCES "use_type_history" ("use_type_id");

ALTER TABLE "unit_history" ADD FOREIGN KEY ("address_id") REFERENCES "address_history" ("address_id");

ALTER TABLE "address_history" ADD FOREIGN KEY ("address_id") REFERENCES "legal_entity_history" ("address_id");

ALTER TABLE "permit_history" ADD FOREIGN KEY ("permit_id_type") REFERENCES "permit_type_history" ("permit_type_id");

ALTER TABLE "permit_history" ADD FOREIGN KEY ("applicant_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "permit_history" ADD FOREIGN KEY ("contractor_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel_history" ("parcel_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("building_id") REFERENCES "building_history" ("building_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("unit_id") REFERENCES "unit_history" ("unit_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("owner_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("division_id") REFERENCES "division_history" ("division_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("service_type_id") REFERENCES "service_type_history" ("service_type_id");

ALTER TABLE "service_history" ADD FOREIGN KEY ("fee_id") REFERENCES "fee_history" ("fee_id");

ALTER TABLE "condemnation_history" ADD FOREIGN KEY ("inspection_id") REFERENCES "inspection_history" ("inspection_id");

ALTER TABLE "inspection_history" ADD FOREIGN KEY ("building_id") REFERENCES "building_history" ("building_id");

ALTER TABLE "inspection_history" ADD FOREIGN KEY ("inspection_type_id") REFERENCES "inspection_type_history" ("inspection_type_id");

ALTER TABLE "assessment_history" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel_history" ("parcel_id");

ALTER TABLE "tax_delinquency_history" ADD FOREIGN KEY ("legal_entity_id") REFERENCES "legal_entity_history" ("legal_entity_id");

ALTER TABLE "municipal_parcel_id_mapping_history" ADD FOREIGN KEY ("parcel_id") REFERENCES "parcel_history" ("parcel_id");

ALTER TABLE "municipal_parcel_id_mapping_history" ADD FOREIGN KEY ("municipality_id") REFERENCES "municipality_history" ("municipality_id");
