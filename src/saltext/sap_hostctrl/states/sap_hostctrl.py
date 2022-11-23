"""
SaltStack extension for SAP Host Agent
Copyright (C) 2022 SAP UCC Magdeburg

SAP Host Agent state module
===========================
SaltStack module that implements states based on SAP Host Agent functionality.
:maintainer:    Benjamin Wegener, Alexander Wilke
:maturity:      new
:depends:       requests
:platform:      Linux

This module implements states that utilize SAP Host Agent functionality.

.. note::
    Because functions are called over SOAP, only authenticated requests are accepted.

Currently, only basic authentication (username/password) is implemented.

.. note::
    This module can only run on linux platforms.
"""
import glob
import logging
import os
import re

import requests
import salt.utils.files
import salt.utils.http
from packaging import version
from requests.auth import HTTPBasicAuth

# Globals
log = logging.getLogger(__name__)

__virtualname__ = "sap_hostctrl"

HOST_AGENT_DIR = "/usr/sap/hostctrl"


# pylint: disable=unused-argument
def system_installed(name, password, username="sapadm", **kwargs):
    """
    Checks if an SAP system is installed over the SAP Host Agent function ListSystems.

    name
        SID of the SAP system.

    password
        Password for the user that executes SAP Host Agent commands (see ``username``).

    username
        User that executes SAP Host Agent commands, default is ``sapadm``.

    This state can be used to initially check if an SAP system is installed before
    running many other states against it.

    Example:
    .. code-block:: jinja

        SAP System S4H is installed:
          sap_hostctrl.system_installed:
            - name: S4H
            - username: sapadm
            - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": "",
    }
    if name in __salt__["sap_hostctrl.list_systems"](username=username, password=password):
        ret["comment"] = f"SAP system {name} is installed"
        ret["result"] = True
    else:
        ret["comment"] = f"SAP system {name} is not installed"
        ret["result"] = False
    return ret


# pylint: disable=unused-argument
def outside_discovery_executed(
    name,
    sld_port,
    sld_user,
    sld_password,
    password,
    username="sapadm",
    overwrite=False,
    keep_other_config=False,
    **kwargs,
):
    """
    Ensure that SAP Host Agent is configured to use outside discovery and is executed. This state
    can manage multiple SLD / LMDB configurations, see parameter ``keep_other_config`` for more
    information.

    name
        SLD / LMDB fully qualified domain name.

    sld_port
        Port of the SLD / LMDB.

    sld_username
        Username used for authentication against the SLD / LMDB.

    sld_password
        Password used for authentication against the SLD / LMDB.

    password
        Password for the user that executes SAP Host Agent commands (see ``username``).

    username
        User that executes SAP Host Agent commands, default is ``sapadm``.

    overwrite
        Overwrite the SLD configuration even if the correct configuration is already set, default is ``False``.

    keep_other_config
        If set to True, other SLD configurations are not removed. Default is ``False``.

    .. warning:
        You need to set the parameter ``service/trace = 2`` in ``host_profile`` in order for the SAP Host Agent to write
        logs that contain information about the result of the last executed outside discovery. This behavior differs
        between CLI and SOAP exection. If you do not set the parameter, the outside discovery will always be executed.

    Example:
    .. code-block:: jinja

        Outside Discovery is executed:
          sap_hostctrl.outside_discovery_executed:
            - name: sol.my.domain
            - sld_port: 50000
            - sld_username: SLD_DS_USER
            - sld_password: __slot__:salt:vault.read_secret(path="SAP", key="SLD_DS_USER")
            - username: sapadm
            - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {
            "new": [],
            "old": [],
        },
        "result": False,
        "comment": "",
    }

    sldreg_dir = f"{HOST_AGENT_DIR}/exe"
    sldreg_bin = f"{sldreg_dir}/sldreg"
    conf = f"{HOST_AGENT_DIR}/exe/config.d/slddest_{name}_{sld_port}.cfg"
    log_file = f"{HOST_AGENT_DIR}/work/outsidediscovery.log"

    if not keep_other_config:
        log.debug(
            f"Checking and removing other SLD configurations in {HOST_AGENT_DIR}/exe/config.d/"
        )
        sld_configs = glob.glob(f"{HOST_AGENT_DIR}/exe/config.d/slddest_*.cfg")
        if conf in sld_configs:
            sld_configs.remove(conf)
        for sld_cfg in sld_configs:
            if __opts__["test"]:
                ret["changes"]["old"].append(f"Would remove {sld_cfg}")
            else:
                os.remove(sld_cfg)
                ret["changes"]["old"].append(f"Removed {sld_cfg}")

    log.debug("Check if SAP Host Agent is configured to use outside discovery")
    update_cfg = True
    if __salt__["file.file_exists"](conf):
        log.debug("Getting existing config")
        cmd = " ".join([sldreg_bin, "-showconnect", conf])
        result = __salt__["cmd.run_all"](
            cmd=cmd, runas=username, env={"LD_LIBRARY_PATH": sldreg_dir}
        )
        if result["retcode"] != 0:
            return False
        log.debug("Parsing output")
        existing_config = {}
        log.debug(f"Existing config: {result['stdout']}")
        for line in result["stdout"].split("\n"):
            log.debug(f"Line: {line}")
            for param in ["host_param", "https_param", "port_param", "user_param"]:
                log.debug(f"Checking {param}")
                if param in line:
                    line_num = line.find(param)
                    key, value = line[line_num:].split("=", 1)
                    existing_config[key] = value.strip("'")
        log.debug(f"Existing config: {existing_config}")
        if (
            sld_user == existing_config.get("user_param", None)
            and name == existing_config.get("host_param", None)
            and str(sld_port) == str(existing_config.get("port_param", ""))
            and "y" == existing_config.get("https_param", None)
        ):
            update_cfg = False

    if update_cfg:
        ret["changes"]["old"].append("Outside Discovery is not configured correctly")
    else:
        ret["changes"]["old"].append("Outside Discovery is configured correctly")

    if overwrite or update_cfg:
        log.debug("Configure outside discovery")
        if __opts__["test"]:
            ret["changes"]["new"].append("Outside Discovery would be configured")
        else:
            result = __salt__["sap_hostctrl.configure_outside_discovery"](
                name, sld_port, sld_user, sld_password, username, password
            )
            if not isinstance(result, bool) or not result:
                msg = "Cannot configure SAP Host Agent to use outside discovery"
                log.error(msg)
                ret["comment"] = msg
                ret["result"] = False
                return ret
            ret["changes"]["new"].append("Outside Discovery is configured")
    else:
        # Note: the log file is only written if service/trace = 2 is set when running the outside
        # discovery over SOAP!
        log.debug(f"Checking {log_file} for existing outside discovery success")
        re_rc = re.compile(r"Return code: ([0-9]{3})")
        success = True
        log.debug(f"Checking {log_file}")
        try:
            log_file_data = __salt__["file.read"](log_file)
        except FileNotFoundError:
            log.debug(f"{log_file} does not exist")
            success = False
        else:
            return_codes = re_rc.findall(log_file_data)
            log.debug(f"Got result from checkup: {return_codes}")
            if not return_codes or int(return_codes[-1]) != 200:
                log.debug("Could not find any return codes or last return code is not 200")
                success = False
            else:
                log.debug(
                    f"Last return code is {return_codes[-1]}, outside discovery was already successful"
                )

        if success:
            ret["comment"] = "No changes required"
            ret["changes"] = {}
            ret["result"] = None
            return ret
        else:
            log.debug("Outside was not executed successfully, running again")
            ret["changes"]["old"].append("Outside Discovery was not yet executed sucessfully")

    log.debug("Remove old logfile")
    if __opts__["test"]:
        ret["changes"]["old"].append(f"Would remove {log_file}")
    else:
        result = __salt__["file.remove"](log_file)
        if result:
            ret["changes"]["old"].append(f"Removed {log_file}")

    log.debug("Executing outside discovery")
    if __opts__["test"]:
        ret["comment"] = "Outside discovery would be maintained and executed"
        ret["changes"]["new"].append("Outside discovery would be executed")
        ret["result"] = None
    else:
        result = __salt__["sap_hostctrl.execute_outside_discovery"](username, password)
        if not isinstance(result, bool) or not result:
            log.error("Cannot execute outside discovery")
            ret["comment"] = "Outside discovery configuration is maintained but execution failed"
            ret["result"] = False
        else:
            ret[
                "comment"
            ] = "Outside discovery configuration is maintained and was executed successfully"
            ret["changes"]["new"].append("Outside discovery was executed succesfully")
            ret["result"] = True

    return ret


# pylint: disable=unused-argument
def sda_installed(
    name, jvm_arch, password, username="sapadm", verify=True, overwrite=False, **kwargs
):
    """
    Ensures that a Simple Diagnostics Agent is installed on the SAP Host Agent.

    name
        Path to the SDA SAR archive.

    jvm_arch
        Path to the SAPJVM SAR archive.

    password
        Password for the user that executes SAP Host Agent commands (see "username").

    username
        User that executes SAP Host Agent commands, default is "sapadm".

    verify
        If set to False, HTTPS connections will not be verified.

    overwrite
        If set to True, SDA will be installed, even if it was already installed.

    .. note::
        This state will use the CA bundle of the OS to determine the validity of the HTTPS
        connection.

    Example:
    .. code-block:: jinja

        Simple Diagnostics Agent is installed:
          sap_hostctrl.sda_installed:
            - name: /mnt/nfs/SIMDIAGAGNT1SP60P_3-70002252.SAR
            - jvm_arch: /mnt/nfs/SAPJVM8_90-80000202.SAR
            - username: sapadm
            - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")
    """
    log.debug("Running function")
    ret = {
        "name": name,
        "changes": {},
        "result": False,
        "comment": "",
    }
    fqdn = __grains__["fqdn"]
    session = requests.Session()
    if verify:
        session.verify = salt.utils.http.get_ca_bundle()
    else:
        session.verify = False
    session.auth = HTTPBasicAuth(username, password)
    sda_inst = False

    if not overwrite:
        log.debug("Checking if SDA is already installed")
        url = f"https://{fqdn}:1129/lmsl/sda/default/?service=ping"
        response = session.get(url)
        log.trace(f"Raw response:\n{response.text}")
        if response.ok:
            # if we can retrieve and parse the version info, then SDA is installed
            try:
                sda_info = response.json()
                version.parse(sda_info["software"])
            except:  # pylint: disable=bare-except
                pass
            else:
                sda_inst = True

    if not sda_inst:
        log.debug("SDA is not installed or overwrite set, installing")
        if __opts__["test"]:
            if overwrite:
                ret["changes"] = {
                    "old": "SDA was perhaps installed",
                    "new": "SDA would be installed",
                }
            else:
                ret["changes"] = {"old": "SDA was not installed", "new": "SDA would be installed"}
        else:
            url = f"https://{fqdn}:1129/SMDAgent/deploy"
            with salt.utils.files.fopen(name, "rb") as sda_archive_f, salt.utils.files.fopen(
                jvm_arch, "rb"
            ) as jvm_archive_f:
                files = {
                    "sda-archive": sda_archive_f,
                    "jvm-archive": jvm_archive_f,
                }
                response = session.post(url, files=files)
                log.trace(f"Raw response:\n{response.text}")
                if not response.ok:
                    log.error(f"Could not upload SDA:\n{response.text}")
                    ret["comment"] = "Could not install SDA"
                    ret["changes"] = {}
                    ret["result"] = False
                else:
                    log.debug("Installed SDA")
                    ret["comment"] = "Installed SDA"
                    if overwrite:
                        ret["changes"] = {
                            "old": "SDA was perhaps installed",
                            "new": "SDA is installed",
                        }
                    else:
                        ret["changes"] = {"old": "SDA was not installed", "new": "SDA is installed"}
    else:
        log.debug("SDA is already installed")
        ret["comment"] = "No changes required"
        ret["result"] = True
        ret["changes"] = {}

    ret["result"] = True if (not __opts__["test"] or not ret["changes"]) else None

    return ret
