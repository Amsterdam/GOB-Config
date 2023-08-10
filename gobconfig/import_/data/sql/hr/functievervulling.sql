SELECT
	identificatie,
	langstzittende,
	datum_aanvang,
	datum_einde,
	functie_titel,
	indicatie_statutair,
	schorsing,
	aansprakelijke,
	handelingsbekwaam,
	bevoegdheids_aansprakelijke,
	bestuursfunctie,
	bevoegdheid_bestuurder,
	vertegenwoordiger_bestuurder_rechtspersoon,
	gemachtigde,
	volmacht,
	statutair,
	heeft_hr_vestiging,
	beperkte_volmacht,
	beperking_in_geld,
	doen_van_opgave_aan_handelsregister,
	overige_volmacht,
	omschrijving_overige_beperkingen,
	beperking_in_handeling,
	soort_handeling,
	volledige_volmacht,
	overige_functionaris,
	afwijkend_aansprakelijkheidsbeding,
	bevoegdheid_funtionaris_volgens_buitlands_recht,
	publiekrechtelijke_functionaris,
	bevoegdheid_publiek_rechtelijke_functionaris,
	soort_bevoegdheid,
	functionaris_bijzondere_rechtstoestand
FROM hr_prepared.functievervulling
