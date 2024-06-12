CREATE OR REPLACE FUNCTION get_table_indexes(table_name text)
RETURNS TABLE(index_name text, is_primary boolean, is_secondary boolean, column_name text) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT
            c.relname::text AS index_name,
            i.indisprimary AS is_primary,
            NOT i.indisprimary AS is_secondary,
            a.attname::text AS column_name
        FROM
            pg_class c
        JOIN
            pg_index i ON c.oid = i.indexrelid
        JOIN
            pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE
            i.indrelid = %L::regclass', table_name);
END;
$$ LANGUAGE plpgsql;
