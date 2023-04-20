SELECT
	identificatie,
	volgnummer,
	begin_geldigheid,
	eind_geldigheid,
	einddatum_recht,
	aard_code,
	aard_omschrijving,
	omschrijving,
	betreft_gedeelte_van_perceel,
	heeft_brk_betrokken_persoon,
	heeft_betrekking_op_brk_kadastraal_object,
	is_gebaseerd_op_brk_stukdeel,
	einddatum,
	datum_actueel_tot,
	DATE(toestandsdatum) AS toestandsdatum,
	_expiration_date
FROM brk2_prepared.aantekening_kadastraal_object
