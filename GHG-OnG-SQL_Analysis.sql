--SQL command used: import (create, copy), where, group by, nest query, having, aggregate, extract year
-- Table: public.asset_oil_gas_production_transport_emissions

-- DROP TABLE IF EXISTS public.asset_oil_gas_production_transport_emissions;

CREATE TABLE IF NOT EXISTS public.asset_oil_gas_production_transport_emissions
(
    id integer NOT NULL DEFAULT nextval('asset_oil_gas_production_transport_emissions'::regclass),
    asset_id character varying(50) COLLATE pg_catalog."default",
    iso3_country character varying(50) COLLATE pg_catalog."default",
    original_inventory_sector character varying(50) COLLATE pg_catalog."default",
    start_time date,
    end_time date,
    temporal_granularity character varying(50) COLLATE pg_catalog."default",
    gas character varying(50) COLLATE pg_catalog."default",
    emissions_quantity double precision,
    emissions_factor double precision,
    emissions_factor_units character varying COLLATE pg_catalog."default",
    created_date date,
    modified_date date,
    asset_name character varying COLLATE pg_catalog."default",
    asset_type character varying COLLATE pg_catalog."default",
    st_astext character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.asset_oil_gas_production_transport_emissions
    OWNER to postgres;
	
--IMPORT csv files to table##
COPY asset_oil_gas_production_transport_emissions 
	(asset_id,
	iso3_country,
	original_inventory_sector,
	start_time,
	end_time,
	temporal_granularity,
	gas,
	emissions_quantity,
	emissions_factor,
	emissions_factor_units,
	created_date,
	modified_date,
	asset_name,
	asset_type,
	st_astext)
FROM 'C:\Program Files\PostgreSQL\fossil_fuel_operations\asset_oil-and-gas-production-and-transport_emissions.csv'
DELIMITER ','
CSV HEADER;

/*--Objective: 
1 create new query for co2 sum emission (2015-2021) by country
2 create new query for average sum emission by country (2015-2021)
3 find the country having more than and less than average emission (2015-2021)
4 create new query for co2 sum emission (2015-2021) divided by asset type (oil/gas) group by country
5 create new query for co2 sum emission each year group by country
*/

--checking table limit to 10
SELECT * 
FROM asset_oil_gas_production_transport_emissions
LIMIT 10;

/*--sum emission group by gas type where country is india
SELECT sum(emissions_quantity) as sum_emissions, gas
FROM asset_oil_gas_production_transport_emissions
WHERE iso3_country = 'IND' 
GROUP BY gas;

--sum emission type group by asset type and only co2 gas
SELECT asset_type, gas, sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE iso3_country = 'IND' AND gas = 'co2' 
GROUP BY asset_type, gas;*/

--sum emissions_quantitiy co2 per country

SELECT sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY iso3_country;

--avg emission_quantity co2 (from sub query)
SELECT avg(sum_emissions)
FROM (SELECT sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY iso3_country) as sum_emission_co2;

--Selecting country and co2 sum emission having more than average
SELECT iso3_country as COUNTRY, sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY COUNTRY, gas
HAVING sum(emissions_quantity) > (SELECT avg(sum_emissions)
									FROM (SELECT sum(emissions_quantity) as sum_emissions
									FROM asset_oil_gas_production_transport_emissions
									WHERE gas = 'co2'
									GROUP BY iso3_country) as sum_emission_co2)
ORDER BY sum_emissions DESC;

--Selecting country and co2 sum emission having less than average
SELECT iso3_country as COUNTRY, sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY COUNTRY, gas
HAVING sum(emissions_quantity) < (SELECT avg(sum_emissions)
									FROM (SELECT sum(emissions_quantity) as sum_emissions
									FROM asset_oil_gas_production_transport_emissions
									WHERE gas = 'co2'
									GROUP BY iso3_country) as sum_emission_co2)
ORDER BY sum_emissions ASC;

--checking avaiable asset_type data
SELECT DISTINCT(asset_type)
FROM asset_oil_gas_production_transport_emissions;

--Sum emission group by country, asset type and gas co2
SELECT iso3_country as Country, asset_type, sum(emissions_quantity) as sum_emissions
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY Country, asset_type, gas
ORDER BY Country;

--finding country with max co2 emission from Oil asset type
SELECT Country, sum_emissions
	FROM (SELECT iso3_country as Country, asset_type, sum(emissions_quantity) as sum_emissions
			FROM asset_oil_gas_production_transport_emissions
			WHERE gas = 'co2'
			GROUP BY Country, asset_type, gas
			ORDER BY Country) as sum_type
	WHERE sum_emissions = 
			(SELECT max(sum_emissions)
				FROM (SELECT iso3_country as Country, asset_type, sum(emissions_quantity) as sum_emissions
				FROM asset_oil_gas_production_transport_emissions
				WHERE gas = 'co2'
				GROUP BY Country, asset_type, gas
				ORDER BY Country) as sum_type
				WHERE asset_type = 'Oil');
--answer: USA

--finding country with max co2 emission from Gas asset type
SELECT Country, sum_emissions
	FROM (SELECT iso3_country as Country, asset_type, sum(emissions_quantity) as sum_emissions
			FROM asset_oil_gas_production_transport_emissions
			WHERE gas = 'co2'
			GROUP BY Country, asset_type, gas
			ORDER BY Country) as sum_type
	WHERE sum_emissions = 
			(SELECT max(sum_emissions)
			FROM (SELECT iso3_country as Country, asset_type, sum(emissions_quantity) as sum_emissions
			FROM asset_oil_gas_production_transport_emissions
			WHERE gas = 'co2'
			GROUP BY Country, asset_type, gas
			ORDER BY Country) as sum_type
			WHERE asset_type = 'Gas');
--answer: Rusia
			
--finding country sum emission group by year

SELECT iso3_country as country, EXTRACT(year FROM end_time) as year, sum(emissions_quantity) as sum_emissions 
FROM asset_oil_gas_production_transport_emissions
WHERE gas = 'co2'
GROUP BY country, year
ORDER BY country, year;