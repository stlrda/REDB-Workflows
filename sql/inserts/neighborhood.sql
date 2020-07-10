INSERT INTO history.neighborhood
    (
     neighborhood_name
     ,county_id
    )
    SELECT DISTINCT
                    pp."Nbrhd"
    ,(SELECT county_id FROM history.county WHERE county.county_name = 'Saint Louis City County')
    FROM "staging_1"."prcl_prcl" pp
    WHERE NOT EXISTS(
        SELECT
               neighborhood_name
        FROM history.neighborhood hn
        WHERE pp."Nbrhd" = hn."neighborhood_name"
            AND hn.current_flag = True
        )
    RETURNING *;