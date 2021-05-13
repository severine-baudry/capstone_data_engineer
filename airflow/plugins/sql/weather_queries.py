q_filter_weather = """
            DROP TABLE IF EXISTS {{params.filtered_table}};
            CREATE TABLE {{params.filtered_table}} AS
            WITH temp AS (
                SELECT * FROM {{params.full_table}}
                WHERE quality_flag IS NULL
                )
            SELECT temp.measured, temp.station_id,  temp.date, temp.value 
            FROM temp JOIN {{params.selected_stations}}  AS sel
            ON sel.station_id = temp.station_id AND sel.measured = temp.measured

            """

q_create_stage__weather_table = """
            DROP TABLE IF EXISTS {{params.stage_table}};
            CREATE TABLE IF NOT EXISTS {{params.stage_table}}(
                station_id varchar(12),
                date varchar(8),
                measured varchar(4),
                value int, 
                measurement_flag varchar(1), 
                quality_flag varchar(1), 
                source_flag varchar(1), 
                hour varchar(6)
            );
            
            COPY {{params.stage_table}} 
            FROM \'{{ti.xcom_pull(key='weather_diff_dir')}}/{{params.stage_file}}\' 
            WITH CSV HEADER
            """  

q_process_delete_weather = """
    DELETE FROM weather_data
    USING recent_delete_filtered AS r
    WHERE (weather_data.measured IS NOT DISTINCT FROM r.measured) AND
        (weather_data.station_id IS NOT DISTINCT FROM r.station_id) AND
        (weather_data.date IS NOT DISTINCT FROM r.date) AND
        (weather_data.value IS NOT DISTINCT FROM r.value) 
    """

q_process_insert_weather = """
    INSERT INTO weather_data AS w
    SELECT * FROM recent_insert_filtered 
    ON CONFLICT( measured, station_id, date) DO 
    UPDATE SET value = EXCLUDED.value
    """

q_process_update_weather = """
    INSERT INTO weather_data AS w
    SELECT * FROM recent_update_filtered 
    ON CONFLICT( measured, station_id, date) DO 
    UPDATE SET value = EXCLUDED.value
    """
