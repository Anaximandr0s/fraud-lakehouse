{# Iceberg maintenance for every table of a schema, in the order of docs/architecture.md §5. #}
{% macro maintain_schema(schema) %}
    {% set sorted_tables = ["silver.transactions"] %}
    {% set cutoff = (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S") %}
    {% for table in run_query("show tables in " ~ schema).columns[1].values() %}
        {% set name = schema ~ "." ~ table %}
        {% do log("maintaining " ~ name, info=True) %}
        {# binpack would merge files without sorting; tables with a write order keep it with sort. #}
        {% set strategy = "sort" if name in sorted_tables else "binpack" %}
        {% do run_query("call spark_catalog.system.rewrite_data_files(table => '" ~ name ~ "', strategy => '" ~ strategy ~ "')") %}
        {% do run_query("call spark_catalog.system.rewrite_manifests(table => '" ~ name ~ "')") %}
        {# Procedure arguments must be literals, so the cutoff is computed here. #}
        {% do run_query("call spark_catalog.system.expire_snapshots(table => '" ~ name ~ "', older_than => timestamp '" ~ cutoff ~ "')") %}
        {% do run_query("call spark_catalog.system.remove_orphan_files(table => '" ~ name ~ "')") %}
    {% endfor %}
{% endmacro %}
