Fixed the community build, which failed in the schema dump with
`{not_exported, emqx_authn_ldap_schema, refs}`.

`emqx_conf_schema_inject` named the LDAP authn and authz schema modules in its
community lists, and the application they live in is Business Source Licensed and
so is not part of this Apache distribution. The references are removed, along with
the LDAP entry in the gateway application list and the reboot list.

OpenQTT therefore has no LDAP authentication or authorization. The Apache LDAP
driver, `emqx_ldap`, is unaffected and remains available to connectors.
