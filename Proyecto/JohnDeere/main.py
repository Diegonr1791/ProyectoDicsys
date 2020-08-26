import requests
import pyodbc
import pandas as pd
import numpy
from sqlalchemy import create_engine
import json
from requests_oauthlib import OAuth1


if __name__ == "__main__":

#CONECTAMOS LA API
url = 'https://sandboxapi.deere.com/platform/organizations'
auth = OAuth1('johndeere-JcFsHU6CV0klrsgkDHewTLuSAP1QZ2Q8Tx9sFCOs',
              '5b67d63b58a9e80facb93354b18a79eb9be3b668f02e331cace8f5a4190254fa',
               'd8d3c1b7-8f56-4cc3-96a6-0b6da898eea6',
                '19Ryi+uXOFxS3/8Q0h+pHjlGVXaYndLhTOl64Wg4cdA4/5ly0f48DxcN+nBsfFgXP71ruvCHMb4T5fYXeGzQ5uAzGYDKyfONgE0Mp6y1bbA=')
headers = {"Accept":"application/vnd.deere.axiom.v3+json"}
## VER param del get
##r = req.get(url, headers, auth=auth)'
r = requests.get('https://sandboxapi.deere.com/platform/organizations',headers=headers,auth=auth)
print(r.status_code)
print(r.headers['Content-Type'])
r = r.json()
print(r['values']) 




path = 'D:\Diego\Dicsys\Proyecto\JohnDeere\\'

#REALIZAMOS LA CONEXION A LA BASE DE DATOS
server = 'DIEGO-PC\MSSQLSERVER1'
database = 'JohnDeere' 
#username = 'sa' 
#password = '' 
conexion = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database)
cursor = conexion.cursor()


"""INSERTAMOS LAS ORGANIZACIONES """    

file = 'Organizaciones\organizaciones.json'
with open(path + file) as contenido :
    resultado = json.load(contenido)
    org = resultado['values']

for res in org:
    print(res['name'],',',res['type'],',',res['member'],',',res['id'])   
    name = str(res['name'])
    tipo = str(res['type'])
    member = str(res['member'])
    id = str(res['id'])
    cursor.execute("INSERT INTO ORGANIZACION (id,name,tipo,member) values(?,?,?,?)", id,name,tipo,member)

"""INSERTAMOS LAS MAQUINAS """
"""
filas =['Maquinas\org-300612.json','Maquinas\org-439957.json','Maquinas\org-439962.json']
for f in filas:
    with open(path + f) as contenido :
        resultado = json.load(contenido)
        maquinas = resultado['values']

    for res in maquinas:
        id = str(res['id'])   
        name = str(res['name'])
        visualizationCategory = str(res['visualizationCategory'])
        machineCategory = res['machineCategories']
        productKey = str(res['productKey'])
        engineSerialNumber = str(res['engineSerialNumber'])
        telematicState = str(res['telematicsState'])
        capabilities = str(res['capabilities'])
        terminals = str(res['terminals'])
        displays = str(res['displays'])
        guid1 = str(res['GUID'])
        modelYear = str(res['modelYear'])
        vin = str(res['vin'])
        cursor.execute("INSERT INTO MAQUINAS (id,visualizationCategory,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,GUID1,modelYear,vin,name) values(?,?,?,?,?,?,?,?,?,?,?,?)", id,visualizationCategory,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,guid1,modelYear,vin,name)
        
        #insertamos los datos en la tabla make
        make = res['make']
        idmake = make['id']
        namemake = make['name']
        cursor.execute("INSERT INTO MAKE (id,name) VALUES(?,?)",idmake,namemake)
        
        #insertamos los datos en la tabla model
        model = res['model']
        idmodel = model['id']
        namemodel = model ['name']
        cursor.execute("INSERT INTO MODEL (id,name) VALUES(?,?)",idmodel,namemodel)
        
        #insertamos los datos en la tabla categoria
        category = res['category']
        idcat = category['id']
        namecat = category ['name']
        cursor.execute("INSERT INTO CATEGORIA (id,name) VALUES(?,?)",idcat,namecat)

        #insertamos los datos en la tabla DMC
        dmc = res['detailMachineCode']
        namedmc = dmc ['name']
        cursor.execute("INSERT INTO dmc (name) VALUES(?)",namedmc)
"""
"""INSERTAMOS LAS ALERTAS """ 


"""INSERTAMOS LOS INFORMES DE ESTADO DISPOSITIVO DE CADA MAQUINA"""
"""
filas =['\Maquinas\Informe_estado_disp\iedm-315205.json','\Maquinas\Informe_estado_disp\iedm-384159.json','\Maquinas\Informe_estado_disp\iedm-616560.json','\Maquinas\Informe_estado_disp\iedm-618225.json']
for f in filas:
    with open(path + f) as contenido :
        resultado = json.load(contenido)
        info_estado = resultado['values']

    for res in info_estado:   
        tiempo = str(res['time'])
        gatewayType = str(res['visualizationCategory'])
        location = res['machineCategories']
        minRSSI = str(res['productKey'])
        maxRSSI = str(res['engineSerialNumber'])
        averageRSSI = str(res['telematicsState'])
        gpsFixTimestamp = str(res['capabilities'])
        engineState = str(res['terminals'])
        terminalPowerState = str(res['displays'])
        cellModemState = str(res['GUID'])
        cellModemAntennaState = str(res['modelYear'])
        gpsModemState = str(res['modelYear'])
        gpsAntennaState = str(res['modelYear'])
        gpsError = str(res['modelYear'])
        gpsFirmawareLevelError = str(res['modelYear'])
        network = str(res['modelYear'])
        rssi = str(res['modelYear'])
        vin = str(res['vin'])
        #cursor.execute("INSERT INTO MAQUINAS (tiempo,gatewayType,location,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,gpsError,gpsFirmawareLevelError,network,rssi) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", id,visualizationCategory,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,guid1,modelYear,vin,name)
        print()
"""
conexion.commit()
cursor.close()
conexion.close()



"""
    #insertamos los datos en la tabla make
    make = res['make']
    idmake = make['id']
    namemake = make['name']
    #cursor.execute("INSERT INTO MAKE (id,name) VALUES(?,?)",idmake,namemake)
        
    #insertamos los datos en la tabla model
    model = res['model']
    idmodel = model['id']
    namemodel = model ['name']
    #cursor.execute("INSERT INTO MODEL (id,name) VALUES(?,?)",idmodel,namemodel)

    #insertamos los datos en la tabla categoria
    category = res['category']
    idcat = category['id']
    namecat = category ['name']
    #cursor.execute("INSERT INTO CATEGORIA (id,name) VALUES(?,?)",idcat,namecat)

    #insertamos los datos en la tabla DMC
    dmc = res['detailMachineCode']
    namedmc = dmc ['name']
    #cursor.execute("INSERT INTO dmc (name) VALUES(?)",namedmc)
    """







tiempo = str(res['time'])
gatewayType = str(res['gatewayType'])
location = res['location']
minRSSI = str(res['minRSSI'])
maxRSSI = str(res['maxRSSI'])
averageRSSI = str(res['averageRSSI'])
gpsFixTimestamp = str(res['gpsFixTimestamp'])
engineState = str(res['engineState'])
terminalPowerState = str(res['terminalPowerState'])
cellModemState = str(res['cellModemState'])
cellModemAntennaState = str(res['cellModemAntennaState'])
gpsModemState = str(res['gpsModemState'])
gpsAntennaState = str(res['gpsAntennaState'])
gpsError = str(res['gpsError'])
gpsFirmawareLevelError = str(res['gpsFirmawareLevelError'])
network = str(res['network'])
rssi = str(res['rssi'])
cursor.execute("INSERT INTO MAQUINAS (tiempo,gatewayType,location,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,gpsError,gpsFirmawareLevelError,network,rssi) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tiempo,gatewayType,location,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,gpsError,gpsFirmawareLevelError,network,rssi)