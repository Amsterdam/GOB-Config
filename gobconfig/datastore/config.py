import os

from gobconfig.exception import GOBConfigException

TYPE_ORACLE = 'oracle'
TYPE_POSTGRES = 'postgres'
TYPE_OBJECTSTORE = 'objectstore'

DATASTORE_CONFIGS = {
    'Grondslag': {
        'type': TYPE_ORACLE,
        'username': os.getenv("DBIMB_DATABASE_USER", "gob"),
        'password': os.getenv("DBIMB_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIMB_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIMB_DATABASE_PORT", 1521),
        'database': os.getenv("DBIMB_DATABASE", "")
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
        'username': os.getenv("BINNRN_DATABASE_USER", "gob"),
        'password': os.getenv("BINNRN_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("BINNRN_DATABASE_HOST", "hostname"),
        'port': os.getenv("BINNRN_DATABASE_PORT", 1521),
        'database': os.getenv("BINNRN_DATABASE", ""),
    },
    'Decos': {
        'type': TYPE_ORACLE,
        'username': os.getenv("DBIDC_DATABASE_USER", "gob"),
        'password': os.getenv("DBIDC_DATABASE_PASSWORD", "insecure"),
        'host': os.getenv("DBIDC_DATABASE_HOST", "hostname"),
        'port': os.getenv("DBIDC_DATABASE_PORT", 1521),
        'database': os.getenv("DBIDC_DATABASE", ""),
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
        'username': os.getenv('ANALYSE_DATABASE_USER'),
        'password': os.getenv('ANALYSE_DATABASE_PASSWORD'),
        'host': os.getenv('ANALYSE_DATABASE_HOST_OVERRIDE'),
        'port': os.getenv('ANALYSE_DATABASE_PORT_OVERRIDE'),
        'database': os.getenv('ANALYSE_DATABASE'),
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
    }
}


def get_datastore_config(name: str) -> dict:
    try:
        config = DATASTORE_CONFIGS[name].copy()
    except KeyError:
        raise GOBConfigException(f"Datastore config for source {name} not found.")

    config['name'] = name
    return config
