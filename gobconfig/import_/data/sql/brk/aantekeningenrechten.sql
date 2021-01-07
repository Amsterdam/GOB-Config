SELECT brk_atg_id AS identificatie,
       nrn_atg_id,
       atg_aardaantekening_code,
       atg_aardaantekening_oms,
       atg_omschrijving,
       atg_einddatum,
       brk_sjt_ids,
       nrn_tng_ids,
       nrn_sdl_ids,
       toestandsdatum,
       expiration_date
FROM brk_prepared.aantekening_recht