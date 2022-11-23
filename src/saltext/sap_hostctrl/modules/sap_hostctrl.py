"""
SaltStack extension for SAP Host Agent
Copyright (C) 2022 SAP UCC Magdeburg

SAP Host Agent execution module
===============================
SaltStack execution module that wraps SAP Host Agent functions.
:maintainer:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       zeep, requests
:platform:      Linux

This module wraps different functions of the SAP Host Agent by calling the corresponding SOAP services.
For controlling the state of the SAP Host Agent itself, use the service module.

By default, the functions will try to connect to the SAP Host Agent over HTTPS on port 1129 and can
optionally fall back to HTTP communication on port 1128.

.. note::
    Because functions are called over SOAP, only authenticated requests are accepted.

Currently, only basic authentication (username/password) is implemented.

.. note::
    This module was only tested on linux platforms.
"""
import logging

import salt.utils.files

# Third Party libs
ZEEPLIB = True
REQUESTSLIB = True
try:
    from zeep import Client
    from zeep.transports import Transport
except ImportError:
    ZEEPLIB = None
    REQUESTSLIB = True
try:
    from requests.exceptions import SSLError
    from requests.auth import HTTPBasicAuth
    from requests import Session
except ImportError:
    REQUESTSLIB = None

# Globals
log = logging.getLogger(__name__)
logging.getLogger("zeep").setLevel(logging.WARNING)  # data from here is not really required

__virtualname__ = "sap_hostctrl"

HOST_AGENT_DIR = "/usr/sap/hostctrl"


def __virtual__():
    """
    Only load this module if all libraries are available.
    """
    if not REQUESTSLIB:
        return False, "Could not load SAP hostctrl module, requests unavailable"
    if not ZEEPLIB:
        return False, "Could not load SAP hostctrl module, zeep unavailable"
    if not __salt__["file.directory_exists"](f"{HOST_AGENT_DIR}/exe/"):
        return False, "SAP Host Agent is not installed on this host"
    return __virtualname__


def _get_client(username, password, fallback=True, fqdn=None, timeout=300):
    """
    Creates and returns a SOAP client.

    This is **not** identical to sap_control._get_client()
    """
    if not fqdn:
        fqdn = __grains__["fqdn"]

    session = Session()
    session.verify = salt.utils.http.get_ca_bundle()
    session.auth = HTTPBasicAuth(username, password)
    transport = Transport(session=session, timeout=timeout, operation_timeout=timeout)
    url = f"https://{fqdn}:1129/SAPHostControl/?wsdl"
    log.debug(f"Retrieving services from {url}")
    client = None
    try:
        client = Client(url, transport=transport)
    except SSLError as ssl_exception:
        log.debug(f"Got an exception:\n{ssl_exception}")
        if "certificate verify failed" in ssl_exception.__str__():
            log.error(f"Could not verify SSL certificate of {fqdn}")
        else:
            log.error(f"Cannot setup connection to Host Agent on {fqdn}")
        client = False
    except Exception as exc:  # pylint: disable=broad-except
        log.debug(f"Got an exception:\n{exc}")
        log.error(f"Cannot setup connection to Host Agent on {fqdn}")

    if fallback and not client:
        log.warning("HTTPS connection failed, trying  over an unsecure HTTP connection!")
        session.verify = False
        url = f"http://{fqdn}:1128/SAPHostControl/?wsdl"
        try:
            client = Client(url, transport=transport)
        except Exception as exc:  # pylint: disable=broad-except
            # possible exceptions unclear / undocumented
            log.debug(f"Got an exception:\n{exc}")
            log.error(f"Cannot setup connection to Host Agent on {fqdn}")
            return False

    if client:
        # pylint: disable=protected-access
        client.service._binding_options["address"] = client.service._binding_options[
            "address"
        ].replace("localhost", fqdn, 1)
    return client


# pylint: disable=unused-argument
def list_systems(username, password, fallback=True, fqdn=None, **kwargs):
    """
    Lists all SAP systems on the host. Should only be used for SAP System
    detection.

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.list_systems username="sapadm" password="Abcd1234"
    """
    log.debug("Running list_systems")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service ListInstances")
    response = client.service.ListInstances(aSelector={"aInstanceStatus": "S-INSTALLED"})
    if response:
        log.trace(f"Raw response:\n{response.text}")
    result = []
    if not isinstance(response, list):
        log.warning("No systems found")
    else:
        for instance in response:
            if not instance["mSid"] in result:
                result.append(instance["mSid"])
    return result


# pylint: disable=unused-argument
def list_instances(sid, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Retrieves all instances for a given SID.

    sid
        SAP System ID for which all instances should be retrieved.

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.list_instances sid="S4H" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service ListInstances")
    response = client.service.ListInstances(aSelector={"aInstanceStatus": "S-INSTALLED"})
    if response:
        log.trace(f"Raw response:\n{response.text}")
    result = []
    if not isinstance(response, list):
        log.warning("No instances found")
    else:
        for instance in response:
            if instance["mSid"] == sid:
                result.append({instance["mHostname"]: instance["mSystemNumber"]})

    return result


# pylint: disable=unused-argument
def list_database_systems(username, password, fallback=True, fqdn=None, **kwargs):
    """
    Lists all database systems on the host. The returned information will include
    all database instances and connection information for each database.

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.list_database_systems username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    if not fqdn:
        fqdn = __grains__["fqdn"]
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    # pylint: disable=protected-access
    client.service._binding_options["address"] = client.service._binding_options["address"].replace(
        "localhost", fqdn, 1
    )
    log.debug("Calling service ListDatabases")
    response = client.service.ListDatabaseSystems(aArguments=[])
    if response:
        log.trace(f"Raw response:\n{response.text}")
    result = []
    if not isinstance(response, list):
        log.warning("No database found")
    else:
        for database in response:
            db_data = {"instances": []}
            for db_prop in database["mDatabase"]["item"]:
                if db_prop["mKey"] == "Database/Name":
                    db_data["name"] = db_prop["mValue"]
                elif db_prop["mKey"] == "Database/Type":
                    db_data["type"] = db_prop["mValue"]
                elif db_prop["mKey"] == "Database/Release":
                    db_data["version"] = db_prop["mValue"]
            for db_prop in database["mProperties"]["item"]:
                if db_prop["mKey"] == "ConnectAddress":
                    db_data["connect_string"] = db_prop["mValue"]
            for instance in database["mInstances"]["item"]:
                ins = {}
                for in_prop in instance["mInstance"]["item"]:
                    if in_prop["mKey"] == "Database/InstanceName":
                        ins["name"] = in_prop["mValue"]
                    elif in_prop["mKey"] == "Database/Host":
                        ins["host"] = in_prop["mValue"]
                if ins:
                    db_data["instances"].append(ins)
            if "name" in db_data:
                result.append(db_data)
        log.trace(f"Processed result:\n{result}")
    return result


# pylint: disable=unused-argument
def get_database_status(dbname, dbtype, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Retrieve the status of a database.

    dbname
        Name of the database, usually the identifier, e.g. HAN

    dbtype
        Type of the database, can be but is not limited to: ada, db6, hdb

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.get_database_status dbname="S4H" dbtype="hdb" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    if not fqdn:
        fqdn = __grains__["fqdn"]
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    # pylint: disable=protected-access
    client.service._binding_options["address"] = client.service._binding_options["address"].replace(
        "localhost", fqdn, 1
    )
    log.debug("Calling service GetDatabaseStatus")
    response = client.service.GetDatabaseStatus(
        aArguments={
            "item": [
                {"mKey": "Database/Type", "mValue": dbtype},
                {"mKey": "Database/Name", "mValue": dbname},
            ]
        }
    )
    if response:
        log.trace(f"Raw response:\n{response.text}")
    if not response or "status" not in response:
        log.error("Could not determine database status")
        return False
    return response["status"]


# pylint: disable=unused-argument
def start_database(dbname, dbtype, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Starts a database.

    .. note:
        For the database type "hdb", all databases will be started

    dbname
        Name of the database, usually the identifier, e.g. HAN

    dbtype
        Type of the database, can be but is not limited to: ada, db6, hdb

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.start_database dbname="S4H" dbtype="hdb" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service StartDatabase")
    response = client.service.StartDatabase(
        aArguments={
            "item": [
                {"mKey": "Database/Type", "mValue": dbtype},
                {"mKey": "Database/Name", "mValue": dbname},
            ]
        },
        aOptions={"mTimeout": 300, "mSoftTimeout": 180, "mOptions": {}},
    )
    if response:
        log.trace(f"Raw response:\n{response.text}")
    if not response["mOperationResults"]:
        log.error("A timeout occured")
        return False
    for result in response["mOperationResults"]["item"]:
        if (
            result["mMessageKey"] == "LogMsg/Text"
            and result["mMessageValue"] == "StartDatabase successfully executed"
        ):
            return True
    return False


# pylint: disable=unused-argument
def stop_database(dbname, dbtype, username, password, fallback=True, fqdn=None, **kwargs):
    """
    Stops a database

    dbname
        Name of the database, usually the identifier, e.g. HAN

    dbtype
        Type of the database, can be but is not limited to: ada, db6, hdb

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.stop_database dbname="S4H" dbtype="hdb" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service StopDatabase")
    response = client.service.StopDatabase(
        aArguments={
            "item": [
                {"mKey": "Database/Type", "mValue": dbtype},
                {"mKey": "Database/Name", "mValue": dbname},
            ]
        },
        aOptions={"mTimeout": 300, "mSoftTimeout": 180, "mOptions": {}},
    )
    if response:
        log.trace(f"Raw response:\n{response.text}")
    if not response["mOperationResults"]:
        log.error("A timeout occured")
        return False
    for result in response["mOperationResults"]["item"]:
        if (
            result["mMessageKey"] == "LogMsg/Text"
            and result["mMessageValue"] == "StopDatabase successfully executed"
        ):
            return True
    return False


# pylint: disable=unused-argument
def configure_outside_discovery(
    sld_host,
    sld_port,
    sld_username,
    sld_password,
    username,
    password,
    fqdn=None,
    fallback=True,
    **kwargs,
):
    """# pylint: disable=line-too-long
    Configure the outside discovery for the SAP Host Agent, i.e. write the SLD configuration.

    sld_host
        SLD / LMDB fully qualified domain name.

    sld_port
        Port of the SLD / LMDB.

    sld_username
        Username used for authentication against the SLD / LMDB.

    sld_password
        Password used for authentication against the SLD / LMDB.

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.configure_outside_discovery sld_host="sol.my.domain" sld_port="50000" sld_username="SLD_DS_USER" sld_password="Abcd1234" username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service ConfigureOutsideDiscovery")
    response = client.service.ConfigureOutsideDiscovery(
        configuration={
            "flags": {},
            "destinations": {
                "item": [
                    {
                        "host": sld_host,
                        "port": int(sld_port),
                        "username": sld_username,
                        "password": sld_password,
                        "useSSL": True,
                    }
                ]
            },
            "arguments": {},
        }
    )
    if response:
        log.trace(f"Raw response:\n{response.text}")
    else:
        log.error("An error occured")
        return False

    success = False
    for member_item in getattr(response, "mMembers", {}).values():
        if hasattr(member_item, "mProperties") and hasattr(member_item.mProperties, "item"):
            for item in member_item.mProperties.item:
                if item.mName == "SLDRegistration":
                    success = getattr(item, "mValue", "ERROR") == "Enabled"
                    break
    return success


def execute_outside_discovery(username, password, fqdn=None, fallback=True, **kwargs):
    """
    Execute the outside discovery, i.e. send the data. This requires the outside discovery
    to be properly configured.

    username
        The username to use for authentication against the SOAP service.

    password
        The password to use for authentication against the SOAP service.

    fallback
        Switch to allow falling back to HTTP communication if no valid
        HTTPS connection can be created (e.g. due to invalid certificates).
        Default is ``True``.

    fqdn
        FQDN of the SAP Host Agent to connect to. Default is the current
        FQDN.

    CLI Example:

    .. code-block:: bash

        salt "*" sap_hostctrl.configure_outside_discovery username="sapadm" password="Abcd1234"
    """
    log.debug("Running function")
    client = _get_client(username=username, password=password, fallback=fallback, fqdn=fqdn)
    if not client:
        return False
    log.debug("Calling service ExecuteOutsideDiscovery")
    response = client.service.ExecuteOutsideDiscovery(
        aArguments={},
        mOptions={
            "item": [
                "OD-EXECUTESLDREG",
            ]
        },
    )
    if response:
        log.trace(f"Raw response:\n{response.text}")
    else:
        log.error("An error occured")
        return False

    success = False
    for prop in response:
        if hasattr(prop, "mProperties") and hasattr(prop.mProperties, "item"):
            for item in prop.mProperties.item:
                if item.mName == "SLDREGStatus":
                    success = getattr(item, "mValue", "ERROR") == "OK"
                    break
    return success
