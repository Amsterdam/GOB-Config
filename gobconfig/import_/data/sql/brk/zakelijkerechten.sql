SELECT
   id,
   identificatie,
   rust_op_kadastraalobj_volgnr as volgnummer,
   aardzakelijkrecht_code,
   aardzakelijkrecht_oms,
   aardzakelijkrecht_akr_code,
   is_belast_met,
   belast,
   ontstaan_uit,
   ontstaan_uit_ref,
   betrokken_bij,
   betrokken_bij_ref,
   isbeperkt_tot,
   nrn_asg_id,
   asg_app_rechtsplitstype_code,
   asg_app_rechtsplitstype_oms,
   asg_einddatum,
   asg_actueel,
   rust_op_kadastraalobject_id,
   rust_op_kadastraalobj_volgnr,
   kadastraal_object_id as rust_op_kadastraal_object_identificatie,
   zrt_einddatum,
   zrt_begindatum,
   kot_status_code,
   toestandsdatum,
   expiration_date
FROM brk_prepared.zakelijk_recht