DO
$$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'  and tablename <> 'ericsson_5g_enrichment'
    LOOP
        EXECUTE format('TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE;', 'public', tbl);
    END LOOP;
END
$$;
