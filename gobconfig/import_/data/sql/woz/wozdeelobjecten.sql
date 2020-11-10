select wozdeelobjectnummer,
       deelnummer,
       wozbelobjnr,
       soort_code,
       soort_omschrijving,
       begin_geldigheid,
       is_verbonden_met_verblijfsobject,
       is_verbonden_met_ligplaats,
       is_verbonden_met_standplaats,
       heeft_pand
from woz_prepared.deelobject
