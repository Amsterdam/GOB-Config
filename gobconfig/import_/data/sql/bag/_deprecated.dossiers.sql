SELECT *
FROM   (
   SELECT regexp_substr(b.subject2, '^(.*?)[_ ]', 1, 1, NULL, 1)         AS dossier
   ,      MAX(b.item_created) AS registratiedatum
   ,      rtrim( xmlcast( xmlagg( xmlelement(e, b.subject2 || ';') order by b.subject2) as clob), ';') AS heeft_bag_brondocument
   FROM   decos.item b
   JOIN   decos.item d ON d.item_key = b.it_parent_key
   WHERE  d.item_deleted IS NULL
   AND    b.item_deleted IS NULL
   AND    b.subject2 IS NOT NULL
   AND    UPPER(substr(b.subject1, 8, 2)) IN ('GV', 'MB', 'PC', 'RO', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SJ', 'SK', 'SM', 'SN', 'SP', 'SQ', 'SR', 'ST', 'SU', 'SV', 'SW', 'TM')
   GROUP BY regexp_substr(b.subject2, '^(.*?)[_ ]', 1, 1, NULL, 1)
   )