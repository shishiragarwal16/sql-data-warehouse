/* CREATING DATABASES AND SCHEMAS

This script creates a db DataWareHouse and three level architecture Schemas- bronze, silver, gold

*/

USE master;

Create DATABASE DataWareHouse;

USE DataWareHouse;
GO
CREATE Schema bronze;
GO
CREATE Schema silver;
GO
CREATE Schema gold;
GO

