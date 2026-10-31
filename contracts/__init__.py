"""Data contracts (resource package, no code).

* ``bronze_contract.json`` - Bronze tables, metadata columns, Parquet codec; also read by
  Terraform (Glue Catalog) and checked against dbt sources.
* ``consumer_contract.json`` - consumer products, columns, types and keys.
* ``*.xsd`` - schemas of the XML deliveries.

Typed access: ``erl_protocol.contracts``. Change a contract first, then the code that follows it.
"""
