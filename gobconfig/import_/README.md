# Import definitions

The JSON import definition is expected to provide a mapping for every attribute present in GOBModel. When an
attribute is NOT provided by the import, because it is simply not available, or because the attribute is enriched
in a later stage, this attribute should be added to the `not_provided_attributes` list. This way the Data Consistency
Tests can safely ignore this attribute.
The keys in `gob_mapping` and the attributes in `not_provided_attributes` together should contain all the attributes
defined in GOBModel, meaning that each and every attribute is either provided or explicitly not provided.
