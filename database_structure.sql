-- SQL compatible with MySQL v5.5.12
--
-- Can run this with the following command:
--     mysql --user=user_name --password=your_password db_name < your_sql_file.sql
--
-- MySQL column type sizes and ranges:
--     UNSIGNED TINYINT can hold 0 - 255 (1 byte)
--     UNSIGNED SMALLINT can hold 0 - 65,536 (2 bytes)
--     UNSIGNED INT can hold 0 to 4,294,967,295 (4 bytes)
--     FLOAT can hold 6 - 9 significant digits (4 bytes)
--     DOUBLE can hold 15 - 17 significant digits (8 bytes)
--     BINARY(16) can hold a uuid, a huge integer, or 16 characters (ALA uses uuids)
--     ENUM takes up 1 byte if there are less than 255 values


-- Each species has many records, and each record belongs to one species.
CREATE TABLE IF NOT EXISTS `species` (
    `id` SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `scientific_name` VARCHAR(256) NOT NULL,
    `common_name` VARCHAR(256) NOT NULL
);


-- Each row represents a data source of records (e.g. ALA).
-- Each source has many records, and each record belongs to one source.
CREATE TABLE IF NOT EXISTS `sources` (
    `id` TINYINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(256) NOT NULL
        COMMENT 'arbitrary human-readble identifier for the source',
    `last_import_time` DATETIME NULL
        COMMENT 'the last time data was imported from this source'
);


-- Each row is an occurrence record.
--
-- This table will hold around 16 million records from ALA alone, 
-- so this table should have as few columns as possible.
--
-- Maybe add a "has_user_ratings" column as an optimisation, so that
-- you don't have to make a separate query to find out if the
-- record has any user ratings.
--
-- TODO: find out how precise lat/longs need to be (float or double)
CREATE TABLE IF NOT EXISTS `records` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `latitude` FLOAT NOT NULL,
    `longitude` FLOAT NOT NULL,
    `rating` ENUM('good','suspect','bad') NOT NULL,
    `species_id` SMALLINT UNSIGNED NOT NULL
        COMMENT 'foreign key to species.id',
    `source_id` TINYINT UNSIGNED NOT NULL 
        COMMENT 'foreign key to sources.id',
    `source_record_id` BINARY(16) NULL 
        COMMENT 'the id of the record as obtained from the source (e.g. the uuid from ALA).',
                 
    INDEX `species_id_index` (species_id)
)
-- MyISAM should theoretically be faster than InnoDB
-- for tables that are not updated or inserted frequently.
-- MyISAM doesn't do foreign key constraints, unfortunately.
ENGINE=MyISAM
PACK_KEYS=1
ROW_FORMAT=FIXED;


-- TODO: add extra info required per user
CREATE TABLE IF NOT EXISTS `users` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `email` VARCHAR(256) NOT NULL
);


-- These are the user ratings (a.k.a vetting information) for
-- occurrence records. ALA has a system of "assertions" that
-- doesn't match up very will to the way we will be rating
-- records. Has a many-to-many relationship with the 'records'
-- table via the 'records_ratings_bridge' table.
CREATE TABLE IF NOT EXISTS `ratings` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `user_id` INT UNSIGNED NOT NULL
        COMMENT 'foreign key into users.id',
    `comment` TEXT NOT NULL
        COMMENT 'additional free-form comment supplied by the user',
    `rating` ENUM('good', 'suspect', 'bad') NOT NULL
        COMMENT 'user supplied rating. same enum as "records.rating"'
);
         

-- Bridging table between 'records' and 'ratings'
CREATE TABLE IF NOT EXISTS `records_ratings_bridge` (
    `record_id` INT UNSIGNED NOT NULL,
    `rating_id` INT UNSIGNED NOT NULL,
    
    PRIMARY KEY (`record_id`, `rating_id`)
)
COMMENT='Bridging table between "records" and "ratings"';
