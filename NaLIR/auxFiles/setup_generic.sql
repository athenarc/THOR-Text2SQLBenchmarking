USE YELP;

-- Create a procedure to load automatically the sizes to the size table
drop PROCEDURE if exists load_size_data;

-- Change the end cmd delimiter to #
DELIMITER #

-- This procedure creates a size table that sotres the table names 
-- and the number of rows each table contains.
CREATE PROCEDURE load_size_data(IN schema_name VARCHAR(255))
begin  
  DECLARE t_name VARCHAR(255);
  DECLARE t_counter INT UNSIGNED default 0;
  DECLARE t_number INT;

  -- Create the size table used to store the 
  -- sizes of each table in the selected database.
  DROP TABLE IF EXISTS size;
  CREATE TABLE size(
    size INT,
    relation VARCHAR(255)
  );  
  
  -- Get the Number of tables in the database.  
  SELECT 
    COUNT(*) INTO t_number
  FROM 
    INFORMATION_SCHEMA.TABLES 
  WHERE  
    table_schema = (select schema_name);

  -- Start a transaction
  -- START TRANSACTION;  
     
  -- Loop all the Tables
  WHILE t_counter < t_number do

    -- Get the table's Name
    SELECT 
      table_name INTO t_name
    FROM
      INFORMATION_SCHEMA.TABLES
    WHERE 
      table_schema = (select schema_name)
    LIMIT
      t_counter, 1;    

    -- Insert the size and the table name to the size table.
    Set @n = CONCAT('''', t_name, '''');
    SET @q = CONCAT('INSERT INTO size SELECT COUNT(*),',  @n , ' from ', t_name); 
    PREPARE stmt1 FROM @q; 
    EXECUTE stmt1; 
    DEALLOCATE PREPARE stmt1; 
    
    -- Update the counter
    SET t_counter = t_counter+1;
  END WHILE;

  -- Commit the changes 
  -- COMMIT;

end #

-- Change the end cmd delimiter back to ;
delimiter ;

-- Call the Procedure.
call load_size_data('YELP');

-- Create History table
DROP TABLE IF EXISTS history;
CREATE TABLE history(
    content VARCHAR(1000)
);


select * from size;
