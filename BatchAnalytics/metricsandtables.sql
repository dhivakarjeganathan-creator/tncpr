DROP TABLE IF EXISTS metricsandtables;

CREATE TABLE metricsandtables (
    metricname  text,
    tablename   text
);

-- Insert only columns with at least one non-null value
DO $$
DECLARE 
    r record;
    qry text;
BEGIN
    FOR r IN 
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
    LOOP
        qry := format(
            'INSERT INTO metricsandtables(tablename, metricname)
             SELECT %L, %L
             WHERE EXISTS (SELECT 1 FROM %I.%I WHERE %I IS NOT NULL LIMIT 1);',
            r.table_name, r.column_name,
            'public', r.table_name, r.column_name
        );

        EXECUTE qry;
    END LOOP;
END $$;

INSERT INTO metricsandtables (tablename, metricname)
SELECT DISTINCT
    'ruleexecutionresults',
    LOWER(REPLACE(udc_config_name, '.', '_'))
FROM ruleexecutionresults
WHERE udc_config_value IS NOT NULL;