SELECT
	identificatie,
	id AS neuron_id,
	was_identificatie,
	aard_code,
	aard_omschrijving,
	bedrag_transactie_valuta,
	bedrag_transactie_bedrag,
	is_bron_voor_brk_tenaamstelling::jsonb,
	is_bron_voor_brk_aantekening_kadastraal_object::jsonb,
	is_bron_voor_brk_aantekening_recht::jsonb,
	is_bron_voor_brk_zakelijk_recht::jsonb,
	stukidentificatie,
	portefeuillenummer_akr,
	tijdstip_aanbieding_stuk,
	reeks_code,
	reeks_omschrijving,
	volgnummer_stuk,
	registercode_stuk_code,
	registercode_stuk_omschrijving,
	soort_register_stuk_code,
	soort_register_stuk_omschrijving,
	deel_soort_stuk,
	DATE(toestandsdatum) AS toestandsdatum,
	tekening_ingeschreven,
	tijdstip_ondertekening,
	toelichting_bewaarder,
	datum_actueel_tot,
	_expiration_date,
	is_bron_voor_brk_erfpachtcanon::jsonb
FROM brk2_prepared.stukdeel
