CREATE OR REPLACE FUNCTION public.prcl11_builder(cityblock character varying, parcel character varying, ownercode character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
BEGIN
RETURN concat(lpad((replace(cityblock,'.',cast('' as varchar))),6,'0'), lpad(parcel,4,'0'), ownercode);
END;
$function$
;
