{% macro generate_schema_name(custom_schema_name, node) -%}

    {#-
        Override dbt's default behavior of prepending the profile's default schema.

        Default: <default_schema>_<custom_schema>  →  "staging_intermediate" (wrong)
        This:    <custom_schema>                   →  "intermediate"          (correct)

        If no custom schema is set (e.g. seeds or one-off models), fall back to the
        target default schema defined in profiles.yml.
    -#}

    {%- if custom_schema_name is none -%}

        {{ target.schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
