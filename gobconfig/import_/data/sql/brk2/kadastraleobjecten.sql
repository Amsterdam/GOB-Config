WITH kot_full AS (
    SELECT
	identificatie,
	volgnummer,
	id,
	registratiedatum,
	kadastrale_aanduiding,
	aangeduid_door_brk_gemeente_code,
	aangeduid_door_brk_gemeente_omschrijving,
	aangeduid_door_brk_kadastralegemeente_code,
	aangeduid_door_brk_kadastralegemeente_omschrijving,
	aangeduid_door_brk_kadastralegemeentecode_code,
	aangeduid_door_brk_kadastralegemeentecode_omschrijving,
	aangeduid_door_brk_kadastralesectie,
	perceelnummer,
	indexletter,
	indexnummer,
	soort_grootte_code,
	soort_grootte_omschrijving,
	grootte,
	soort_cultuur_onbebouwd_code,
	soort_cultuur_onbebouwd_omschrijving,
	soort_cultuur_bebouwd_code,
	soort_cultuur_bebouwd_omschrijving,
	status,
	referentie,
	oudst_digitaal_bekend,
	mutatie_id,
	meettarief_verschuldigd,
	toelichting_bewaarder,
	tijdstip_ontstaan_object,
	hoofdsplitsing_identificatie,
	afwijking_lijst_rechthebbenden,
	indicatie_voorlopige_kadastrale_grens,
	ST_AsText(geometrie) AS geometrie,
	ST_AsText(plaatscoordinaten) AS plaatscoordinaten,
	perceelnummer_rotatie,
	perceelnummer_verschuiving_x,
	perceelnummer_verschuiving_y,
	ST_AsText(bijpijling_geometrie) AS bijpijling_geometrie,
	koopsom,
	koopsom_valutacode,
	koopjaar,
	indicatie_meer_objecten,
	toestandsdatum,
	begin_geldigheid,
	eind_geldigheid,
	in_onderzoek,
	datum_actueel_tot,
	_expiration_date,
	is_ontstaan_uit_brk_g_perceel,
	heeft_een_relatie_met_bag_verblijfsobject,
	is_ontstaan_uit_brk_kadastraalobject
	FROM brk2_prepared.kadastraal_object kot_full
)
SELECT *
FROM kot_full
