{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In prod, use the custom schema directly (no prefix) #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev, prefix with developer schema to avoid collisions #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
