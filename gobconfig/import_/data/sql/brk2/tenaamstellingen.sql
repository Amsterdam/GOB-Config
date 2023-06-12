SELECT
	identificatie,
	volgnummer,
	neuron_id,
	was_identificatie,
	van_brk_kadastraalsubject,
	begin_geldigheid,
	eind_geldigheid,
	datum_actueel_tot,
	aandeel_teller,
	aandeel_noemer,
	geldt_voor_teller,
	geldt_voor_noemer,
	burgerlijke_staat_ten_tijde_van_verkrijging_code,
	burgerlijke_staat_ten_tijde_van_verkrijging_omschrijving,
	betrokken_partner_brk_subject,
	betrokken_samenwerkingsverband_brk_subject,
	verkregen_namens_samenwerkingsverband_type,
	verkregen_namens_samenwerkingsverband_omschrijving,
	betrokken_gorzen_en_aanwassen_brk_subject,
	in_onderzoek,
	van_brk_zakelijk_recht,
	is_gebaseerd_op_brk_stukdeel AS is_gebaseerd_op_brk_stukdelen,
	DATE(toestandsdatum) AS toestandsdatum,
	_expiration_date
FROM brk2_prepared.tenaamstelling
