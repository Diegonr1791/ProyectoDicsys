/*Creacion Base de datos*/
Create database JohnDeere

Use JohnDeere
/*Creacion Tablas*/

/*Tabla Organizacion*/
CREATE TABLE ORGANIZACION(

id varchar(50),
name varchar(50),
tipo varchar(50),
member bit,
flag bit,
PRIMARY KEY(id)
);

/*Tablas relacionadas con Maquinas*/
CREATE TABLE MAQUINAS(

id varchar(50),
visualizationCategory varchar(50),
machineCategory varchar(50),
make varchar(50),
model varchar(50),
detailMachineCode varchar(50),
productKey varchar(50),
engineSerialNumber VARCHAR(50),
telematicState VARCHAR(50),
capabilities VARCHAR(50),
terminals VARCHAR(50),
displays VARCHAR(50),
GUID1 VARCHAR(100),
modelYear VARCHAR(50),
vin VARCHAR(50),
name VARCHAR(50),
idorg varchar(50),
flag bit,
PRIMARY KEY(id)
);



CREATE TABLE MAQUINAS_ALERTAS(

id varchar(50),
tipo varchar(50),
duration varchar(50),
ocurrences VARCHAR(50),
engineHours VARCHAR(100),
machineLinearTime VARCHAR(50),
bus VARCHAR(50),
definicion VARCHAR(400),
tiempo VARCHAR(100),
location VARCHAR(100),
color VARCHAR(50),
severity VARCHAR(50),
acknowledgementStatus VARCHAR(50),
ignored varchar(10),
invisible varchar(10),
idmaquina varchar(50),
flag bit,
PRIMARY KEY(id)
);

CREATE TABLE MAQUINAS_INFO_EST_DISP(

id int identity(1,1),
tiempo varchar(100),
gatewayType varchar(100),
location VARCHAR(200),
minRSSI VARCHAR(100),
maxRSSI VARCHAR(100),
averageRSSI VARCHAR(100),
gpsFixTimestamp VARCHAR(100),
engineState varchar(100),
terminalPowerState varchar(100),
cellModemState varchar(100),
cellModemAntennaState varchar(100),
gpsModemState varchar(100),
gpsAntennaState varchar(100),
gpsError varchar(100),
gpsFirmwareLevelError varchar(100),
network varchar(100),
rssi VARCHAR(100),
idmaquina varchar(50),
flag bit,
PRIMARY KEY(id)
);

CREATE TABLE MAQUINAS_HORAS_DE_MOTOR(

id INT IDENTITY(1,1),
value float,
unite varchar(50),
reportTime varchar(50),
idmaquina varchar(50),
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE MAQUINAS_HORAS_DE_OPERACION(

id INT IDENTITY(1,1),
startDate varchar(50),
endDate varchar(50),
engineState int,
idmaquina varchar(50),
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE MAQUINAS_HISTORIAL_UBICACION(

id INT IDENTITY(1,1),
point varchar(200),
eventTimestamp varchar(50),
gpsFixTimestamp varchar(50),
idmaquina varchar(50),
flag bit,
PRIMARY KEY(id) 
);

/*--------------------------*/
CREATE TABLE CLIENTES(

id VARCHAR(50),
name varchar(50),
idorg varchar(50),
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE GRANJAS(

id VARCHAR(50),
name varchar(50),
idcliente varchar(50),
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE CAMPOS(

id VARCHAR(50),
name varchar(50),
idgranja varchar(50),
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE OPERACIONES(

id varchar(50),
fieldOperationType varchar(200),
cropSeason varchar(50),
modifiedTime varchar(50),
startDate varchar(50),
endDate varchar(50),
cropName varchar(50),
idorg int,
idfield varchar(50),
PRIMARY KEY(id) 
);

CREATE TABLE LIMITES(

id varchar(50),
name varchar(50),
sourceType varchar(50),
createdTime VARCHAR(50),
modifiedTime VARCHAR(50),
area VARCHAR(50),
workableArea VARCHAR(50),
multipolygons VARCHAR(50),
extent VARCHAR(50),
archived bit,
active bit,
irrigated bit,
flag bit,
PRIMARY KEY(id) 
);

CREATE TABLE SEEDING(
measurementName varchar(50),
measurementCategory varchar(50),
areaValue float,
areaUnit varchar(50),
totalMaterialValue float,
totalMaterialUnit varchar(50),
averageMaterialValue float,
averageMaterialUnit varchar(50),
idoperacion varchar(100),
flag bit
);

CREATE TABLE HARVEST(
measurementName varchar(50),
areaValue float,
areaUnit varchar(50),
averageYieldValue float,
averageYieldUnit varchar(50),
averageMoistureValue float,
averageMoistureUnit varchar(50),
wetMassValue float,
wetMassUnit varchar(50),
averageWetMassValue float,
averageWetMassUnit varchar(50),
averageSpeedValue float,
averageSpeedUnit varchar(50),
idoperacion varchar(50),
flag bit
);

select * from ORGANIZACION
select * from MAQUINAS
select * from MAQUINAS_INFO_EST_DISP
select * from MAQUINAS_ALERTAS
select * from MAQUINAS_HORAS_DE_MOTOR 
select * from MAQUINAS_HORAS_DE_OPERACION
select * from MAQUINAS_HISTORIAL_UBICACION
select * from GRANJAS
select * from CLIENTES
select * from CAMPOS 
select * from OPERACIONES
select * from SEEDING
select * from HARVEST


truncate table ORGANIZACION
truncate table MAQUINAS_INFO_EST_DISP
truncate table MAQUINAS_HORAS_DE_MOTOR
truncate table MAQUINAS_HORAS_DE_OPERACION
truncate table MAQUINAS_HISTORIAL_UBICACION
truncate table MAQUINAS_INFO_EST_DISP
truncate table MAQUINAS_ALERTAS
truncate table CLIENTES
truncate table GRANJAS
truncate table CAMPOS
truncate table MAQUINAS

delete  from MAQUINAS_HORAS_DE_MOTOR WHERE flag = 1

SELECT TOP 1 * FROM MAQUINAS_ALERTAS
ORDER BY id DESC

SELECT AVG(value) as promedio FROM MAQUINAS_HORAS_DE_MOTOR