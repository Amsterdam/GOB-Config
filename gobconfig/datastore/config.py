import os

from gobconfig.exception import GOBConfigException

TYPE_ORACLE = 'oracle'
TYPE_POSTGRES = 'postgres'
TYPE_OBJECTSTORE = 'objectstore'
TYPE_SQL_SERVER = 'sqlserver'
TYPE_SFTP = 'sftp'
TYPE_BAG_EXTRACT = 'bagextract'

DATASTORE_CONFIGS = {
    'Grondslag': {
        'type': TYPE_ORACLE,
        'username': os.getenv("GRONDSLAG_DATABASE_USER", "gob"),
        'password': os.getenv("GRONDSLAG_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("GRONDSLAG_DATABASE_HOST", "hostname"),
        'port': os.getenv("GRONDSLAG_DATABASE_PORT", 1521),
        'database': os.getenv("GRONDSLAG_DATABASE", "")
    },
    'DGDialog': {
        'type': TYPE_ORACLE,
        'username': os.getenv("BINBG_DATABASE_USER", "gob"),
        'password': os.getenv("BINBG_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINBG_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINBG_DATABASE_PORT", 1521),
        'database': os.getenv("BINBG_DATABASE", "")
    },
    'DIVA': {
        'type': TYPE_ORACLE,
        'username': os.getenv("DBIGM_DATABASE_USER", "gob"),
        'password': os.getenv("DBIGM_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIGM_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIGM_DATABASE_PORT", 1521),
        'database': os.getenv("DBIGM_DATABASE", ""),
    },
    'Neuron': {
        'type': TYPE_ORACLE,
        # NRBIN is the new Neuron configuration. Not to be confused with the old BINNRN variables
        'username': os.getenv("NRBIN_DATABASE_USER", "gob"),
        'password': os.getenv("NRBIN_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("NRBIN_DATABASE_HOST", "hostname"),
        'port': os.getenv("NRBIN_DATABASE_PORT", 1521),
        'database': os.getenv("NRBIN_DATABASE", ""),
    },
    'DecosDeprecated': {
        'type': TYPE_ORACLE,
        'username': os.getenv("DBIDC_DATABASE_USER", "gob"),
        'password': os.getenv("DBIDC_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIDC_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIDC_DATABASE_PORT", 1521),
        'database': os.getenv("DBIDC_DATABASE", ""),
    },
    'Decos': {
        'type': TYPE_SQL_SERVER,
        'username': os.getenv("BINF_DATABASE_USER", "gob"),
        'password': os.getenv("BINF_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINF_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINF_DATABASE_PORT", 1433),
        'database': os.getenv("BINF_DATABASE", ""),
    },
    'GOBBagExtract': {
        'type': TYPE_POSTGRES,
        'username': os.getenv("GOB_BAG_EXTRACT_DATABASE_USER", "gob"),
        'password': os.getenv("GOB_BAG_EXTRACT_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("GOB_BAG_EXTRACT_DATABASE_HOST", "hostname"),
        'port': os.getenv("GOB_BAG_EXTRACT_DATABASE_PORT", 5413),
        'database': os.getenv("GOB_BAG_EXTRACT_DATABASE", 'gob_bagextract'),
    },
    'GOBPrepare': {
        'type': TYPE_POSTGRES,
        'username': os.getenv("GOB_PREPARE_DATABASE_USER", "gob"),
        'password': os.getenv("GOB_PREPARE_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("PREPARE_DATABASE_HOST_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_HOST", "hostname")),
        'port': os.getenv("PREPARE_DATABASE_PORT_OVERRIDE", os.getenv("GOB_PREPARE_DATABASE_PORT", 5408)),
        'database': os.getenv("GOB_PREPARE_DATABASE", ""),
    },
    'GOBAnalyse': {
        'type': TYPE_POSTGRES,
        'username': os.getenv('ANALYSE_DATABASE_USER', "gob"),
        'password': os.getenv('ANALYSE_DATABASE_PASSWORD', "insecure"),
        'host': os.getenv('ANALYSE_DATABASE_HOST_OVERRIDE', "hostname"),
        'port': os.getenv('ANALYSE_DATABASE_PORT_OVERRIDE', 5409),
        'database': os.getenv('ANALYSE_DATABASE', "gob_analyse"),
    },
    'GOBDatabase': {
        'type': TYPE_POSTGRES,
        'username': os.getenv("GOB_DATABASE_USER", "gob"),
        'database': os.getenv("GOB_DATABASE_NAME", "gob"),
        'password': os.getenv("GOB_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("GOB_DATABASE_HOST_OVERRIDE", "localhost"),
        'port': os.getenv("GOB_DATABASE_PORT_OVERRIDE", 5406),
    },
    'Basisinformatie': {
        'type': TYPE_OBJECTSTORE,
        'VERSION': '2.0',
        'AUTHURL': 'https://identity.stack.cloudvps.com/v2.0',
        'TENANT_NAME': os.getenv('BASISINFORMATIE_OBJECTSTORE_TENANT_NAME'),
        'TENANT_ID': os.getenv('BASISINFORMATIE_OBJECTSTORE_TENANT_ID'),
        'USER': os.getenv('BASISINFORMATIE_OBJECTSTORE_USER'),
        'PASSWORD': os.getenv('BASISINFORMATIE_OBJECTSTORE_PASSWORD'),
        'REGION_NAME': 'NL'
    },
    'GOBObjectstore': {
        'type': TYPE_OBJECTSTORE,
        "VERSION": '2.0',
        "AUTHURL": 'https://identity.stack.cloudvps.com/v2.0',
        "TENANT_NAME": os.getenv("GOB_OBJECTSTORE_TENANT_NAME"),
        "TENANT_ID": os.getenv("GOB_OBJECTSTORE_TENANT_ID"),
        "USER": os.getenv("GOB_OBJECTSTORE_USER"),
        "PASSWORD": os.getenv("GOB_OBJECTSTORE_PASSWORD"),
        "REGION_NAME": 'NL'
    },
    'GEO_BRIEVENBUS': {
        'type': TYPE_SFTP,
        'username': os.getenv('GEO_BRIEVENBUS_USER'),
        'password': os.getenv('GEO_BRIEVENBUS_PASSWORD'),
        'host': os.getenv('GEO_BRIEVENBUS_HOST'),
        'port': os.getenv('GEO_BRIEVENBUS_PORT'),
    },
    'GEO_BRIEVENBUS_UVA2': {
        'type': TYPE_SFTP,
        'username': os.getenv('GEO_BRIEVENBUS_UVA2_USER'),
        'password': os.getenv('GEO_BRIEVENBUS_UVA2_PASSWORD'),
        'host': os.getenv('GEO_BRIEVENBUS_HOST'),
        'port': os.getenv('GEO_BRIEVENBUS_PORT'),
    },
    'GEO_BRIEVENBUS_BRK_SUBJ': {
        'type': TYPE_SFTP,
        'username': os.getenv('GEO_BRIEVENBUS_BRK_SUBJ_USER'),
        'password': os.getenv('GEO_BRIEVENBUS_BRK_SUBJ_PASSWORD'),
        'host': os.getenv('GEO_BRIEVENBUS_HOST'),
        'port': os.getenv('GEO_BRIEVENBUS_PORT'),
    },
    'GEO_BRIEVENBUS_DIA': {
        'type': TYPE_SFTP,
        'username': os.getenv('GEO_BRIEVENBUS_DIA_USER'),
        'password': os.getenv('GEO_BRIEVENBUS_DIA_PASSWORD'),
        'host': os.getenv('GEO_BRIEVENBUS_HOST'),
        'port': os.getenv('GEO_BRIEVENBUS_PORT'),
    },
    'BAGExtract': {
        'type': TYPE_BAG_EXTRACT
    }
}


def get_datastore_config(name: str) -> dict:
    try:
        config = DATASTORE_CONFIGS[name].copy()
    except KeyError:
        raise GOBConfigException(f"Datastore config for source {name} not found.")

    config['name'] = name
    return config
