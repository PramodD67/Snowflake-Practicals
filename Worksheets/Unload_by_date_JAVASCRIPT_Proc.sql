CREATE OR REPLACE PROCEDURE unload_date_proc()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Get current date in DD-MM-YYYY format
    var dateStr = snowflake.execute({
        sqlText: "SELECT TO_VARCHAR(CURRENT_DATE(), 'DD-MM-YYYY')"
    }).next().getColumnValue(1);

    // Build COPY command
    var copyCmd = `
        COPY INTO @int_stg/${dateStr}
        FROM sales_data
        FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
        OVERWRITE = TRUE;
    `;

    // Execute COPY
    snowflake.execute({ sqlText: copyCmd });

    return 'File unloaded successfully: ' + dateStr;
$$;
