import os
from typing import Optional, Union

from gobconfig.exception import GOBConfigException

TYPE_ORACLE = "oracle"
TYPE_POSTGRES = "postgresql"
TYPE_OBJECTSTORE = "objectstore"
TYPE_SQL_SERVER = "sqlserver"
TYPE_SFTP = "sftp"

DatastoreConfigType = dict[str, Union[Optional[str], int]]
DatastoreConfigsType = dict[str, DatastoreConfigType]

DATASTORE_CONFIGS: DatastoreConfigsType = {
    "Grondslag": {
        "type": TYPE_ORACLE,
        "username": os.getenv("GRONDSLAG_DATABASE_USER", "gob"),
        "password": os.getenv("GRONDSLAG_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("GRONDSLAG_DATABASE_HOST", "hostname"),
        "port": os.getenv("GRONDSLAG_DATABASE_PORT", 1521),
        "database": os.getenv("GRONDSLAG_DATABASE", ""),
    },
    "DGDialog": {
        "type": TYPE_ORACLE,
        "username": os.getenv("BINBG_DATABASE_USER", "gob"),
        "password": os.getenv("BINBG_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("BINBG_DATABASE_HOST", "hostname"),
        "port": os.getenv("BINBG_DATABASE_PORT", 1521),
        "database": os.getenv("BINBG_DATABASE", ""),
    },
    "Neuron": {
        "type": TYPE_ORACLE,
        "username": os.getenv("NRBIN_DATABASE_USER", "gob"),
        "password": os.getenv("NRBIN_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("NRBIN_DATABASE_HOST", "hostname"),
        "port": os.getenv("NRBIN_DATABASE_PORT", 1521),
        "database": os.getenv("NRBIN_DATABASE", ""),
    },
    "Decos": {
        "type": TYPE_SQL_SERVER,
        "username": os.getenv("BINF_DATABASE_USER", "gob"),
        "password": os.getenv("BINF_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("BINF_DATABASE_HOST", "hostname"),
        "port": os.getenv("BINF_DATABASE_PORT", 1433),
        "database": os.getenv("BINF_DATABASE", ""),
    },
    "GOBPrepare": {
        "type": TYPE_POSTGRES,
        "username": os.getenv("GOB_PREPARE_DATABASE_USER", "gob"),
        "password": os.getenv("GOB_PREPARE_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("PREPARE_DATABASE_HOST_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_HOST", "hostname")),
        "port": os.getenv("PREPARE_DATABASE_PORT_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_PORT", 5408)),
        "database": os.getenv("GOB_PREPARE_DATABASE", ""),
    },
    "GOBDatabase": {
        "type": TYPE_POSTGRES,
        "username": os.getenv("GOB_DATABASE_USER", "gob"),
        "database": os.getenv("GOB_DATABASE_NAME", "gob"),
        "password": os.getenv("GOB_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("GOB_DATABASE_HOST_OVERRIDE", "localhost"),
        "port": os.getenv("GOB_DATABASE_PORT_OVERRIDE", 5406),
    },
    "Basisinformatie": {
        "type": TYPE_OBJECTSTORE,
        "VERSION": "2.0",
        "AUTHURL": "https://identity.stack.cloudvps.com/v2.0",
        "TENANT_NAME": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_NAME"),
        "TENANT_ID": os.getenv("BASISINFORMATIE_OBJECTSTORE_TENANT_ID"),
        "USER": os.getenv("BASISINFORMATIE_OBJECTSTORE_USER"),
        "PASSWORD": os.getenv("BASISINFORMATIE_OBJECTSTORE_PASSWORD"),
        "REGION_NAME": "NL",
    },
    "GOBObjectstore": {
        "type": TYPE_OBJECTSTORE,
        "VERSION": "2.0",
        "AUTHURL": "https://identity.stack.cloudvps.com/v2.0",
        "TENANT_NAME": os.getenv("GOB_OBJECTSTORE_TENANT_NAME"),
        "TENANT_ID": os.getenv("GOB_OBJECTSTORE_TENANT_ID"),
        "USER": os.getenv("GOB_OBJECTSTORE_USER"),
        "PASSWORD": os.getenv("GOB_OBJECTSTORE_PASSWORD"),
        "REGION_NAME": "NL",
    },
    "GEO_BRIEVENBUS": {
        "type": TYPE_SFTP,
        "username": os.getenv("GEO_BRIEVENBUS_USER"),
        "password": os.getenv("GEO_BRIEVENBUS_PASSWORD"),
        "host": os.getenv("GEO_BRIEVENBUS_HOST"),
        "port": os.getenv("GEO_BRIEVENBUS_PORT"),
    },
    "GEO_BRIEVENBUS_UVA2": {
        "type": TYPE_SFTP,
        "username": os.getenv("GEO_BRIEVENBUS_UVA2_USER"),
        "password": os.getenv("GEO_BRIEVENBUS_UVA2_PASSWORD"),
        "host": os.getenv("GEO_BRIEVENBUS_HOST"),
        "port": os.getenv("GEO_BRIEVENBUS_PORT"),
    },
    "GEO_BRIEVENBUS_BRK_SUBJ": {
        "type": TYPE_SFTP,
        "username": os.getenv("GEO_BRIEVENBUS_BRK_SUBJ_USER"),
        "password": os.getenv("GEO_BRIEVENBUS_BRK_SUBJ_PASSWORD"),
        "host": os.getenv("GEO_BRIEVENBUS_HOST"),
        "port": os.getenv("GEO_BRIEVENBUS_PORT"),
    },
    "GEO_BRIEVENBUS_DIA": {
        "type": TYPE_SFTP,
        "username": os.getenv("GEO_BRIEVENBUS_DIA_USER"),
        "password": os.getenv("GEO_BRIEVENBUS_DIA_PASSWORD"),
        "host": os.getenv("GEO_BRIEVENBUS_HOST"),
        "port": os.getenv("GEO_BRIEVENBUS_PORT"),
    },
    "BAGExtract": {
        "type": TYPE_POSTGRES,
        "username": os.getenv("GOB_BAGEXTRACT_DATABASE_USER", "gob"),
        "password": os.getenv("GOB_BAGEXTRACT_DATABASE_PASSWORD", "insecure"),
        "host": os.getenv("BAGEXTRACT_DATABASE_HOST_OVERRIDE", os.getenv("GOB_BAGEXTRACT_DATABASE_HOST", "hostname")),
        "port": os.getenv("BAGEXTRACT_DATABASE_PORT_OVERRIDE", os.getenv("GOB_BAGEXTRACT_DATABASE_PORT", 5413)),
        "database": os.getenv("GOB_BAGEXTRACT_DATABASE", "gob_bagextract"),
    },
}


def get_datastore_config(name: str) -> DatastoreConfigType:
    """Return the datastore configuration for a source."""
    try:
        config = DATASTORE_CONFIGS[name].copy()
    except KeyError:
        raise GOBConfigException(f"Datastore config for source {name} not found.")

    config["name"] = name
    return config
