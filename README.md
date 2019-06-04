<b><h1> Our Purpose </h1></b>
<ol>
    <li>To identify trends in music according to region and user demographics.</li>
    <li>To provide the most accurate and up-to-date data to artists,record companies and other data analytics companies.</li>
    <li>To create a pleasurable and intelligent user experience.</li>
</ol>
<b><h1>Our Schema</h1></b>
<h2>How to run Python scripts</h2>
 <ol>
    <li>Open a terminal in the same directory as the Python file.</li>
    <li>run the following command: <b>python <em>nameofpythonfile.py</em></b></li>
 </ol>
 <h2>Fact Table</h2>
 <ol>
    <li>songplays - records in log data associated with song plays i.e. records with page NextSong.</li>
    <ul>songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent</ul>
  </ol>  
<h2>Dimension Tables</h2>
<ol>
    <li>users - users in the app.</li>
    <ul>user_id, first_name, last_name, gender, level</ul>
    <li>songs - songs in music database.</li>
    <ul>song_id, title, artist_id, year, duration,</ul>
    <li>artists - artists in music database.</li>
    <ul>artist_id, name, location, lattitude, longitude</ul>
    <li>time - timestamps of records in songplays broken down into specific units.</li>
    <ul>start_time, hour, day, week, month, year, weekday</ul>
 </ol>
 <b><h1>ETL Pipeline</h1></b>
 <h2>Project Files</h2>
 <ol>
    <li>[create_tables.py](https://github.com/Johnny512/Data_Warehouse_Redshift/blob/master/create_tables.py) - drops and creates the tables above</li>
    <ul>drop_tables function - executes all DROP TABLE commands that were imported from <b><em>drop_table_queries</em></b></ul>
    <ul>create_tables function - executes all CREATE TABLE statements that were imported from <b><em>create_table_queries</em></b></ul>
    <li>etl.py - Extracts raw data from S3 buckets and Transforms data then Loads into Fact and Dimension tables</li>
    <ul>load_staging_tables function - executes all COPY commands imported from <b><em>copy_table_queries</em></b> from S3 buckets</ul>
    <ul>insert_tables function - executes all INSERT commands imported from <b><em>insert_table_queries</em></b> to Fact and Dimension tables</ul>
    <li>sql_queries.py - holds variables with all SQL commands used in the project</li>
    <ul>DROP, CREATE, COPY, INSERT</ul>
</ol>
