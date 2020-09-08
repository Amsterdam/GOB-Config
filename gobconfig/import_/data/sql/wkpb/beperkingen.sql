SELECT b.registernummer                                                                                       AS identificatie --inschrijfnummer
,      b.volgnummer                                                                                           AS volgnummer
,      t.code                                                                                                 AS beperking_code
,      t.naam                                                                                                 AS beperking_omschrijving
,      to_char(b.datum_bekendmaking, 'YYYY-MM-DD')                                                            AS datum_bekendmaking
,      to_char(b.datum_inwerking, 'YYYY-MM-DD HH24:MI:SS')                                                    AS begingeldigheid
,      to_char(b.datum_einde, 'YYYY-MM-DD HH24:MI:SS')                                                        AS eindgeldigheid
,      s.id                                                                                                   AS status_code
,      s.naam                                                                                                 AS status_omschrijving
,      a.id                                                                                                   AS aard_code
,      a.naam                                                                                                 AS aard_omschrijving
,      x.registernummer                                                                                       AS heeft_voorgaande_beperking
,      b.kenmerk                                                                                              AS documentnummer
,      TRIM(NVL(regexp_substr(substr(b.kenmerk, 1, 21), '^(.*?)_', 1, 1, NULL, 1), substr(b.kenmerk, 1, 21))) AS heeft_dossier
,      o.id                                                                                                   AS orgaan_code
,      o.naam                                                                                                 AS orgaan_omschrijving
,      b.afschermenpersoonsgegevens                                                                           AS persoonsgegevens_afschermen
,      to_char(b.creation, 'YYYY-MM-DD HH24:MI:SS')                                                           AS registratiedatum
,      to_char(b.datum_einde, 'YYYY-MM-DD HH24:MI:SS')                                                        AS expirationdate
,      b.id                                                                                                   AS source_id
,      sdo_util.to_wktgeometry(b.geometrie)                                                                   AS geometrie
FROM   G0363_PRB.beperking              b
-- type beperking selecteren
LEFT OUTER JOIN G0363_PRB.beperkingtype t ON b.id_beperkingtype = t.id
-- status selecteren
LEFT OUTER JOIN G0363_PRB.status        s ON b.id_status = s.id
-- aard selecteren
LEFT OUTER JOIN G0363_PRB.aard          a ON b.id_aard = a.id
-- orgaan selecteren
LEFT OUTER JOIN G0363_PRB.orgaan        o ON b.id_orgaan = o.id
-- registernummer van voorgaande beperking selecteren
LEFT OUTER JOIN G0363_PRB.beperking     x ON b.id_voorgaande_beperking = x.id AND b.volgnummer = x.volgnummer
-- select max volgnummer van iedere beperking
LEFT JOIN (SELECT id, MAX(volgnummer) AS volgnummer FROM G0363_PRB.beperking GROUP BY id) m ON m.id = b.id AND m.volgnummer = b.volgnummer
WHERE b.registernummer IS NOT NULL
AND   m.id IS NOT NULL