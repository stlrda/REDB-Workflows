WITH new_address AS 
	(
	SELECT "prcl_test"."OwnerAddr"
		, "prcl_test"."OwnerCity"
		, "prcl_test"."OwnerState"
		, "prcl_test"."OwnerCountry"
		, "prcl_test"."OwnerZIP"		
	FROM staging_1.prcl_test
	LEFT JOIN core."address"
		ON COALESCE("OwnerAddr", ' ') = COALESCE("street_address", ' ')
        AND COALESCE("OwnerCity", ' ') = COALESCE("city", ' ') 
        AND COALESCE("OwnerState", ' ') = COALESCE("state", ' ')
        AND COALESCE("OwnerCountry", ' ') = COALESCE("country", ' ') 
        AND COALESCE("OwnerZIP", ' ') = COALESCE("zip", ' ')
	WHERE "address"."address_id" IS NULL
	)