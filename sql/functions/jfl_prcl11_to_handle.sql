CREATE OR REPLACE FUNCTION public.prcl11_to_handle(prcl11 character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
BEGIN
RETURN "left"(concat(cast(1 as varchar),prcl11), 11);
END; $function$
;
