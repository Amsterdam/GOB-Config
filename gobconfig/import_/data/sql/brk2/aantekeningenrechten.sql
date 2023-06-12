SELECT
	identificatie,
	neuron_id,
	was_identificatie,
	einddatum_recht,
	aard_code,
	aard_omschrijving,
	omschrijving,
	betreft_gedeelte_van_perceel,
	betrokken_brk_tenaamstelling,
	heeft_brk_betrokken_persoon,
	is_gebaseerd_op_brk_stukdeel,
	einddatum,
	datum_actueel_tot,
	DATE(toestandsdatum) AS toestandsdatum,
	_expiration_date
FROM brk2_prepared.aantekening_recht
