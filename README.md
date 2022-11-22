# SaltStack SAP Host Agent extension
This SaltStack extensions allows interfaces SAP Host Agents running on minions.

**THIS PROJECT IS NOT ASSOCIATED WITH SAP IN ANY WAY**

## Installation
Run the following to install the SaltStack SAPCAR extension:
```bash
salt-call pip.install saltext.sap-hostctrl
```
Keep in mind that this package must be installed on every minion that should utilize the states and execution modules.

Alternatively, you can add this repository directly over gitfs
```yaml
gitfs_remotes:
  - https://github.com/SAPUCC/saltext-sap_hostctrl.git:
    - root: src/saltext/sap_hostctrl
```

## Usage
A state using the SAP Host Agent extension looks like this:
```jinja
Outside Discovery is executed:
  ucp_sap_hostctrl.outside_discovery_executed:
    - name: sol.my.domain
    - sld_port: 50000
    - sld_username: SLD_DS_USER
    - sld_password: __slot__:salt:vault.read_secret(path="SAP", key="SLD_DS_USER")
    - username: sapadm
    - password: __slot__:salt:vault.read_secret(path="os", key="sapadm")
```

## Docs
See https://saltext-sap-hostctrl.readthedocs.io/ for the documentation.

## Contributing
We would love to see your contribution to this project. Please refer to `CONTRIBUTING.md` for further details.

## License
This project is licensed under GPLv3. See `LICENSE.md` for the license text and `COPYRIGHT.md` for the general copyright notice.
