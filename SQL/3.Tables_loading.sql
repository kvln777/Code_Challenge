-- Use BULK INSERT to load data from CSV sales_data_final.csv into the sales_data table
BULK INSERT sales_data
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\sales_data_final.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);

-- Use BULK INSERT to load data from CSV merged_data.csv into the sales_geo_info table 
BULK INSERT sales_geo_info
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\merged_data.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);

-- Use BULK INSERT to load data from CSV merged_sales_data_with_weather.csv into the sales_geo_info_final table (temp and weather desciption) 

BULK INSERT sales_geo_info_final
FROM 'C:\Users\kvlns\OneDrive\Desktop\codingchallengecloudsourcedataengineerrole\merged_sales_data_with_weather.csv'  
WITH (
    FIELDTERMINATOR = ',',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = 'UTF-8'        
);