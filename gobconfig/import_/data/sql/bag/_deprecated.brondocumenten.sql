SELECT *
FROM   (
   SELECT b.subject2                                          AS documentnummer
   ,      UPPER(substr(b.subject2, 1, 2))                     AS bronleverancier_code
   ,      CASE UPPER(substr(b.subject2, 1, 2))
           WHEN 'SA' THEN 'Stadsdeel Centrum'
           WHEN 'SC' THEN 'Stadsdeel Westerpark'
           WHEN 'SD' THEN 'Stadsdeel Oud-West'
           WHEN 'SG' THEN 'Stadsdeel Zeeburg'
           WHEN 'SH' THEN 'Stadsdeel Bos en Lommer'
           WHEN 'SJ' THEN 'Stadsdeel De Baarsjes'
           WHEN 'SN' THEN 'Stadsdeel Noord'
           WHEN 'SP' THEN 'Stadsdeel Geuzenveld-Slotermeer'
           WHEN 'SQ' THEN 'Stadsdeel Osdorp'
           WHEN 'SR' THEN 'Stadsdeel Slotervaart'
           WHEN 'ST' THEN 'Stadsdeel Zuidoost'
           WHEN 'SU' THEN 'Stadsdeel Oost-Watergraafsmeer'
           WHEN 'SV' THEN 'Stadsdeel Oud-Zuid'
           WHEN 'SW' THEN 'Stadsdeel Zuideramstel'
           WHEN 'SE' THEN 'Stadsdeel West'
           WHEN 'SF' THEN 'Stadsdeel Nieuw-West'
           WHEN 'SK' THEN 'Stadsdeel Zuid'
           WHEN 'SM' THEN 'Stadsdeel Oost'
           WHEN 'BD' THEN 'Bestuursdienst'
           WHEN 'DW' THEN 'Wonen'
           WHEN 'MB' THEN 'Omgevingsdienst NZKG'
           WHEN 'OG' THEN 'Grond en Ontwikkeling'
           WHEN 'RO' THEN 'Ruimte en Duurzaamheid'
           WHEN 'ZA' THEN 'Zuidas'
           WHEN 'PC' THEN 'PostNL'
           WHEN 'GV' THEN 'Gegevensbeheer (Basisinformatie)'
           WHEN 'TM' THEN 'Terugmelding'
           WHEN 'SB' THEN 'Straatnaambesluit'
           ELSE NULL
          END                                                AS bronleverancier_omschrijving
   ,      UPPER(substr(b.subject2, 12, 2))                   AS typedossier_code
   ,      CASE UPPER(substr(b.subject2, 12, 2))
           WHEN 'OV' THEN 'Omgevingsvergunning'
           WHEN 'BL' THEN 'Lichte bouwvergunning'
           WHEN 'NB' THEN 'Losse nummerbeschikking'
           WHEN 'RB' THEN 'Reguliere nummerbeschikking'
           WHEN 'SB' THEN 'Besluit openbare ruimte'
           WHEN 'SL' THEN 'Sloopvergunning'
           WHEN 'VW' THEN 'Samenvoegingsvergunning'
           WHEN 'SV' THEN 'Splitsingsvergunning'
           WHEN 'OR' THEN 'Verklaring registratie object Wet BAG'
           WHEN 'AC' THEN 'Ambtshalve correctie'
           WHEN 'WO' THEN 'Woningonttrekking zonder omgevingsvergunning'
           WHEN 'TM' THEN 'Terugmelding'
           ELSE NULL
          END                                                AS typedossier_omschrijving
   ,      UPPER(substr(b.subject2, 16, 2))                   AS typebrondocument_code
   ,      CASE UPPER(substr(b.subject2, 16, 2))
           WHEN 'VL' THEN 'Voorloopvel'
           WHEN 'BA' THEN 'Aanvraag omgevingsvergunning voor bouwen/slopen/splitsing/woningonttrekking'
           WHEN 'VG' THEN 'Omgevingsvergunning voor bouwen/slopen/splitsing/woningonttrekking'
           WHEN 'BT' THEN 'Bouwtekening'
           WHEN 'ST' THEN 'Situatietekening'
           WHEN 'SB' THEN 'Besluit openbare ruimte'
           WHEN 'NB' THEN 'Nummerbeschikking / Besluit stand- of ligplaats'
           WHEN 'GC' THEN 'Voorlopige pandgeometrie'
           WHEN 'GM' THEN 'Definitieve geometrie object-GM: 0-99'
           WHEN 'GN' THEN 'Definitieve geometrie object-GN: 100-199'
           WHEN 'GO' THEN 'Definitieve geometrie object-GO: 200-299'
           WHEN 'GP' THEN 'Definitieve geometrie object-GP: 300-399'
           WHEN 'OR' THEN 'Verklaring registratie object Wet BAG'
           WHEN 'ML' THEN 'Statusmelding'
           WHEN 'AT' THEN 'Aantekening (notitie) op een dossier'
           WHEN 'MF' THEN 'Statusformulier niet te registreren'
           WHEN 'AC' THEN 'Ambtshalve correctie'
           WHEN 'TM' THEN 'Terugmelding (in onderzoek zetten)'
           WHEN 'OP' THEN 'Terugmelding (uit onderzoek halen)'
           ELSE NULL
          END                                                AS typebrondocument_omschrijving
   ,      b.item_created                                     AS registratiedatum
   ,      b.item_key                                         AS source_id
   FROM   decos.item b
   JOIN   decos.item d ON d.item_key = b.it_parent_key
   WHERE  d.item_deleted IS NULL
   AND    b.item_deleted IS NULL
   AND    b.subject2 IS NOT NULL
   AND    UPPER(substr(b.subject1, 8, 2)) IN ('GV', 'MB', 'PC', 'RO', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SJ', 'SK', 'SM', 'SN', 'SP', 'SQ', 'SR', 'ST', 'SU', 'SV', 'SW', 'TM')
   )