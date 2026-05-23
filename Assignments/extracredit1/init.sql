USE master;
GO

-- drop it the db exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'extracredit')
BEGIN 
    ALTER DATABASE extracredit SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE extracredit;
END;
GO


CREATE DATABASE extracredit;
GO

USE extracredit;
GO

-- creating file groups 
ALTER DATABASE extracredit ADD FILEGROUP FGV1;
ALTER DATABASE extracredit ADD FILEGROUP FGV2;
ALTER DATABASE extracredit ADD FILEGROUP FGV3;
ALTER DATABASE extracredit ADD FILEGROUP FGV4;
GO

-- CREATIN FILES

-- volume 1
ALTER DATABASE extracredit ADD FILE (
    NAME = 'V1_F1',
    FILENAME = '/var/opt/mssql/data/v1/v1_f1.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
    ) TO FILEGROUP FGV1;

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V1_F2',
    FILENAME = '/var/opt/mssql/data/v1/v1_f2.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
    ) TO FILEGROUP FGV1;
GO

-- volume 2 
ALTER DATABASE extracredit ADD FILE (
    NAME = 'V2_F1',
    FILENAME = '/var/opt/mssql/data/v2/v2_f1.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV2

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V2_F2',
    FILENAME = '/var/opt/mssql/data/v2/v2_f2.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV2

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V2_F3',
    FILENAME = '/var/opt/mssql/data/v2/v2_f3.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV2
GO

-- volume 3 
ALTER DATABASE extracredit ADD FILE (
    NAME = 'V3_F1',
    FILENAME = '/var/opt/mssql/data/v3/v3_f1.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV3

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V3_F2',
    FILENAME = '/var/opt/mssql/data/v3/v3_f2.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV3
GO

-- volume 4
ALTER DATABASE extracredit ADD FILE (
    NAME = 'V4_F1',
    FILENAME = '/var/opt/mssql/data/v4/v4_f1.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV4

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V4_F2',
    FILENAME = '/var/opt/mssql/data/v4/v4_f2.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV4

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V4_F3',
    FILENAME = '/var/opt/mssql/data/v4/v4_f3.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV4

ALTER DATABASE extracredit ADD FILE (
    NAME = 'V4_F4',
    FILENAME = '/var/opt/mssql/data/v4/v4_f4.ndf',
    SIZE = 5MB,
    FILEGROWTH = 5MB
) TO FILEGROUP FGV4
GO

-- PARTITIONS 

CREATE PARTITION FUNCTION pf_T1 (INT)
AS RANGE LEFT FOR VALUES (150);
GO

CREATE PARTITION FUNCTION pf_T2 (INT)
AS RANGE LEFT FOR VALUES (200, 300);
GO

-- partition scheme 

CREATE PARTITION SCHEME ps_T1
AS PARTITION pf_T1
TO (FGV1, FGV2);
GO

CREATE PARTITION SCHEME ps_T2
AS PARTITION pf_T2
TO (FGV1, FGV3, FGV4);
GO

-- CREATING TABLES 

CREATE TABLE T1 (
    id INT,
    value INT
) ON ps_T1(value);
GO

CREATE TABLE T2 (
    id INT,
    value INT
) ON ps_T2(value);
GO
