# Import definitions

The JSON import definition is expected to provide a mapping for every attribute present in GOBModel. When an
attribute is NOT provided by the import, because it is simply not available, or because the attribute is enriched
in a later stage, this attribute should be added to the `not_provided_attributes` list. This way the Data Consistency
Tests can safely ignore this attribute.
The keys in `gob_mapping` and the attributes in `not_provided_attributes` together should contain all the attributes
defined in GOBModel, meaning that each and every attribute is either provided or explicitly not provided.

## Import definition parameters
### force_list
The `force_list` parameter can be when importing a `GOB.JSON`-typed attribute. For example:

    "gob_mapping": {
        ...,
        "gebruiksdoel": {
          "force_list": true,
          "source_mapping": {
            "omschrijving": "gebruiksdoel_import_value"
          }
        },
        ...,
    }

By default, without the `force_list` parameter, the imported value `gebruiksdoel_import_value` would be copied as-is into
a JSON object with a key `omschrijving`. If however, we have a list with two 'gebruiksdoelen', we would need to build
the list in the source and import it as the `source_mapping`. For example, in the source we have:

    gebruiksdoel_import_value = [{"omschrijving": "woonfunctie"}, {"omschrijving": "kantoorfunctie"}]

and our `gob_mapping` would be:

    "gebruiksdoel": {
        "source_mapping": "gebruiksdoel_import_value"
    }

This works for certain sources, but if we're importing from file for example, we can't do this. `force_list` to the
rescue. Take this example:

    gebruiksdoel_import_value = ["woonfunctie", "kantoorfunctie"]

    "gebruiksdoel": {
      "force_list": true,
      "source_mapping": {
        "omschrijving": "gebruiksdoel_import_value"
      }
    },

With `force_list` set to `true`, this value would be parsed exactly as is done for a `GOB.ManyReference`: the imported
list will be unpacked transformed to a list of dicts, so that the result is the same as the result above:


    gebruiksdoel = [{"omschrijving": "woonfunctie"}, {"omschrijving": "kantoorfunctie"}]

This also works (just like with a `GOB.ManyReference`) if the imported value is a single string. This will result in
a list with one single dict with the imported string as `omschrijving`.
