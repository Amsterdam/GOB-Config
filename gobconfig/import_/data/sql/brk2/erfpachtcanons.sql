SELECT
    identificatie,
    volgnummer,
    soort_code,
    soort_omschrijving,
    jaarlijks_bedrag_valuta_som,
    jaarlijks_bedrag_valuta_code,
    jaarlijks_bedrag_betreft_meer_onroerende_zaken,
    einddatum_afkoop,
    indicatie_oude_onroerende_zaken_betrokken,
    is_gebaseerd_op_brk_stukdeel_identificatie,
    datum_actueel_tot,
    DATE(toestandsdatum) AS toestandsdatum,
    betreft_brk_zakelijk_recht_identificatie,
    begin_geldigheid,
    eind_geldigheid
FROM brk2_prepared.erfpachtcanon
