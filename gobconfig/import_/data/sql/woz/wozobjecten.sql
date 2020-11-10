select wozobjectnummer,
       gebruik_code,
       gebruik_oms,
       soortobject_code,
       soortobject_oms,
       begin_geldigheid,
       bevat_kadastraal_object
from woz_prepared.object
