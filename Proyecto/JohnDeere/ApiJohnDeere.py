import requests
import pyodbc
import pandas as pd
import numpy
from sqlalchemy import create_engine
import json
from requests_oauthlib import OAuth1
import random
from datetime import datetime, timedelta

#REALIZAMOS LA CONEXION Y AUTENTICACION CON LA API
url = 'https://sandboxapi.deere.com/platform/organizations'
auth = OAuth1('johndeere-JcFsHU6CV0klrsgkDHewTLuSAP1QZ2Q8Tx9sFCOs',
              '5b67d63b58a9e80facb93354b18a79eb9be3b668f02e331cace8f5a4190254fa',
               'd8d3c1b7-8f56-4cc3-96a6-0b6da898eea6',
                '19Ryi+uXOFxS3/8Q0h+pHjlGVXaYndLhTOl64Wg4cdA4/5ly0f48DxcN+nBsfFgXP71ruvCHMb4T5fYXeGzQ5uAzGYDKyfONgE0Mp6y1bbA=')
headers = {"Accept":"application/vnd.deere.axiom.v3+json"}


#REALIZAMOS LA CONEXION A LA BASE DE DATOS
server = 'DIEGO-PC\MSSQLSERVER1'
database = 'JohnDeere' 
#username = 'sa' 
#password = '' 
conexion = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database)
cursor = conexion.cursor()



"""INSERTAMOS LAS ORGANIZACIONES """    

r = requests.get('https://sandboxapi.deere.com/platform/organizations',headers=headers,auth=auth)
r = r.json()
org = r['values']
id_org = []

flag = 0
for res in org:
    name = str(res['name'])
    tipo = str(res['type'])
    member = str(res['member'])
    id = str(res['id'])
    links = res['links']
    #cursor.execute("INSERT INTO ORGANIZACION (id,name,tipo,member,flag) values(?,?,?,?,?)", id,name,tipo,member,flag)
    id_org.append(id)
    #insertamos los clientes,granjas,campos,limites en cascada
    print('Organizacion ',id,' cargada')
    
    for link in links: 
        if link['rel'] == 'clients':
            
            r_clients = requests.get(link['uri'],headers=headers, auth=auth)
            r_clients = r_clients.json()
            if r_clients['total'] != 0:
                client = r_clients['values']
                for c in client:
                    id_client = str(c['id'])
                    name_client = str(c['name'])
                    #cursor.execute("INSERT INTO CLIENTES (id,name,idorg) values (?,?,?)", id_client,name_client,id)
                    
                    linskfarm = c['links']
                    print('Cliente ',id_client,' cargado')
                    
                    for lf in linskfarm:
                        
                        if lf['rel'] == 'farms':
                            r_farms = requests.get(lf['uri'], headers= headers, auth=auth)
                            r_farms = r_farms.json()
                            if r_farms['total'] != 0:
                                farm = r_farms['values']
                                for f in farm:
                                    id_farm = str(f['id'])
                                    name_farm = str(f['name'])
                                    #cursor.execute("INSERT INTO GRANJAS (id,name,idcliente) values (?,?,?)",id_farm,name_farm,id_client)                                    
                                    print('Granja ',id_farm,' cargada')
                                    
                                    linkfield = f['links']
                                    for lfd in linkfield:
                                        
                                        if lfd['rel'] == 'fields':
                                            r_fields = requests.get(lfd['uri'],headers=headers, auth=auth)
                                            r_fields = r_fields.json()
                                            if r_fields['total'] != 0:
                                                field = r_fields['values']
                                                for fd in field:
                                                    id_field = str(fd['id'])
                                                    name_field = str(fd['name'])
                                                    #cursor.execute("INSERT INTO CAMPOS (id,name,idgranja) values (?,?,?)",id_field,name_field,id_farm)
                                                    
                                                    print('Campo ',id_field,' cargado')                                                   
                                                    linksoperations = fd['links']
                                                    for lo in linksoperations:
                                                        
                                                        if lo['rel'] == 'fieldOperation':
                                                            r_operations = requests.get(lo['uri'],headers = headers, auth=auth)
                                                            r_operations = r_operations.json()
                                                            if r_operations['total'] !=0:
        
                                                                operations = r_operations['values']
                                                                
                                                                for o in operations:
                                                                    id_op = str(o['id'])
                                                                    fieldOperationType = str(o['fieldOperationType'])
                                                                    cropSeason = str(o['cropSeason'])
                                                                    modifiedTime = str(o['modifiedTime'])
                                                                    startDate = str(o['startDate'])
                                                                    endDate = str(o['endDate'])
                                                                    cropName = str(o['cropName'])
                                                                    orgId = o['orgId']
                                                                    idfield = id_field
                                                                    #cursor.execute("INSERT INTO OPERACIONES (id,fieldOperationType,cropSeason,modifiedTime,startDate,endDate,cropName,idorg,idfield) values (?,?,?,?,?,?,?,?,?)",id_op,fieldOperationType,cropSeason,modifiedTime,startDate,endDate,cropName,orgId,idfield)
                                                                    print("Operacion cargada")
                                                                    linksmeasures = o['links']
                                                                    for lm in linksmeasures:
                                                                        if lm['rel'] == 'measurementTypes':
                                                                            r_measures = requests.get(lm['uri'],headers=headers, auth=auth)
                                                                            r_measures = r_measures.json()
                                                                            if r_measures['total'] != 0:
                                                                                measures = r_measures['values']
                                                                                for m in measures:
                                                                                    measurementName = str(m['measurementName'])
                                                                                    print(measurementName)
                                                                                    areaValue = m['area']
                                                                                    areaValue = areaValue['value']
                                                                                    areaUnit = m['area']
                                                                                    areaUnit = areaUnit['unitId']
                                                                                    if  fieldOperationType == 'harvest':
                                                                                        averageYieldValue = m['averageYield']
                                                                                        averageYieldValue = averageYieldValue['value']
                                                                                        averageYieldUnit = m['averageYield']
                                                                                        averageYieldUnit = averageYieldUnit['unitId']
                                                                                        averageMoistureValue = m['averageMoisture']
                                                                                        averageMoistureValue = averageMoistureValue['value']
                                                                                        averageMoistureUnit = m['averageMoisture']
                                                                                        averageMoistureUnit = averageMoistureUnit['unitId']
                                                                                        wetMassValue = m['wetMass']
                                                                                        wetMassValue =wetMassValue['value']
                                                                                        wetMassUnit = m['wetMass']
                                                                                        wetMassUnit = wetMassUnit['unitId']
                                                                                        averageWetMassValue = m['averageWetMass']
                                                                                        averageWetMassValue = averageWetMassValue['value']
                                                                                        averageWetMassUnit = m['averageWetMass']
                                                                                        averageWetMassUnit = averageWetMassUnit['unitId']
                                                                                        averageSpeedValue = m['averageSpeed']
                                                                                        averageSpeedValue = averageSpeedValue['value']
                                                                                        averageSpeedUnit = m['averageSpeed']
                                                                                        averageSpeedUnit = averageSpeedUnit['unitId']
                                                                                        #cursor.execute("INSERT INTO HARVEST(measurementName,areaValue,areaUnit,averageYieldValue,averageYieldUnit,averageMoistureValue,averageMoistureUnit,wetMassValue,wetMassUnit,averageWetMassValue,averageWetMassUnit,averageSpeedValue,averageSpeedUnit,idoperacion,flag) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",measurementName,areaValue,areaUnit,averageYieldValue,averageYieldUnit,averageMoistureValue,averageMoistureUnit,wetMassValue,wetMassUnit,averageWetMassValue,averageWetMassUnit,averageSpeedValue,averageSpeedUnit,id_op,0)
                                                                                        print('Medicion de operacion',id_op,' agregada')
                                                                                    else:
                                                                                        measurementCategory = m['measurementCategory']
                                                                                        if 'totalMaterial' in m:
                                                                                            totalMaterialValue = m['totalMaterial']
                                                                                            totalMaterialValue = totalMaterialValue['value']
                                                                                            totalMaterialUnit = m['totalMaterial']
                                                                                            totalMaterialUnit = totalMaterialUnit['unitId']
                                                                                        else:
                                                                                            totalMaterialValue = None
                                                                                            totalMaterialUnit = None
                                                                                        if 'averageMaterial' in m:
                                                                                            averageMaterialValue = m['averageMaterial']
                                                                                            averageMaterialValue = averageMaterialValue['value']
                                                                                            averageMaterialUnit = m['averageMaterial']
                                                                                            averageMaterialUnit = averageMaterialUnit['unitId']
                                                                                        else:
                                                                                            averageMaterialValue = None
                                                                                            averageMaterialUnit = None
                                                                                        #cursor.execute("INSERT INTO SEEDING (measurementName,areaValue,areaUnit,measurementCategory,totalMaterialValue,totalMaterialUnit,averageMaterialValue,averageMaterialUnit,idoperacion,flag) values (?,?,?,?,?,?,?,?,?,?)",measurementName,areaValue,areaUnit,measurementCategory,totalMaterialValue,totalMaterialUnit,averageMaterialValue,averageMaterialUnit,id_op,0)
                                                                                        print('Medicion de operacion ',id_op,' agregada')
                                                                                        
                                                                    

              

"""INSERTAMOS LAS MAQUINAS"""

id_maquina = []

# a partir del diccionario de ids de las organizaciones iteramos para reccorer cuales tienen maquinas
for id in id_org:
    idorg = id
    r_maq = requests.get('https://sandboxapi.deere.com/platform/organizations/'+ id +'/machines',headers=headers,auth=auth)
    r_maq = r_maq.json()
    #print(r_maq)
    if r_maq['total'] != 0:
        maquinas = r_maq['values']
        #insertamos las maquinas y los datos en tablas alternas
        
        for res in maquinas:
            print("SI ENTRA")
            id_maq = str(res['id'])
            id_maquina.append(id_maq)   
            name = str(res['name'])
            visualizationCategory = str(res['visualizationCategory'])
            
            machineCategory = res['category']
            machineCategory = str('Id: '+ machineCategory['id'] +' Nombre '+ machineCategory['name'])
            make = res['make']
            make = str('Id: '+ make['id'] +' Nombre '+ make['name'])
            model = res['model']
            model = str('Id: '+ model['id'] +' Nombre '+ model['name'])
            dmc = res['detailMachineCode']
            dmc = str(dmc['name'])

            productKey = str(res['productKey'])
            engineSerialNumber = str(res['engineSerialNumber'])
            telematicState = str(res['telematicsState'])
            capabilities = str(res['capabilities'])
            terminals = str(res['terminals'])
            displays = str(res['displays'])
            guid1 = str(res['GUID'])
            modelYear = str(res['modelYear'])
            vin = str(res['vin'])
            #cursor.execute("INSERT INTO MAQUINAS (id,visualizationCategory,machineCategory,make,model,detailMachineCode,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,GUID1,modelYear,vin,name,idorg) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", id_maq,visualizationCategory,machineCategory,make,model,dmc,productKey,engineSerialNumber,telematicState,capabilities,terminals,displays,guid1,modelYear,vin,name,idorg)
           
            
            #itero los links de cada maquina para isertar alerta, informes, etc
            for link in res['links']:
                if link['rel'] == "alerts":
                    
                    r_alerta = requests.get(link['uri'], headers=headers, auth=auth)
                    r_alerta = r_alerta.json()
                    if r_alerta['total'] != 0:
                        alertas = r_alerta['values']
                        #insertamos las alertas 
        
                        for res in alertas:
                            id_aler = str(res['id'])   
                            tipo = str(res['@type'])
                            value = res['duration']
                            value = str(value['valueAsInteger'])
                            unit = res['duration']
                            unit = str(unit['unit'])
                            duration = str(value +' , ' + unit)
                            ocurrences = str(res['occurrences'])
                            valeng = res['engineHours']
                            valeng = valeng['reading']
                            valeng = str(valeng['valueAsDouble'])
                            unieng = res['engineHours']
                            unieng = unieng['reading']
                            unieng = str(unieng['unit'])
                            engineHours = str(valeng + ' , ' + unieng)
                            machineLinearTime = str(res['machineLinearTime'])
                            bus = str(res['bus'])
                            definicion = res['definition']
                            definicion = str(definicion['description'])
                            tiempo = str(res['time'])
                            lat = res['location']
                            lat = str(lat['lat'])
                            lon = res['location']
                            lon = str(lon['lon'])
                            location = str(lat + ' , ' + lon)
                            color = str(res['color'])
                            severity = str(res['severity'])
                            acknowledgementStatus = str(res['acknowledgementStatus'])
                            ignored = str(res['ignored'])
                            invisible = str(res['invisible'])
                            #cursor.execute("INSERT INTO MAQUINAS_ALERTAS (id,tipo,duration,ocurrences,engineHours,machineLinearTime,bus,definicion,tiempo,location,color,severity,acknowledgementStatus,ignored,invisible,idmaquina,flag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", id_aler,tipo,duration,ocurrences,engineHours,machineLinearTime,bus,definicion,tiempo,location,color,severity,acknowledgementStatus,ignored,invisible,id_maq,flag)
                            
                #informes del estado del dispositivo
                elif link['rel'] == 'deviceStateReports':
                    r_ied = requests.get(link['uri'], headers=headers, auth=auth)
                    r_ied = r_ied.json()
                    if r_ied['total'] != 0:
                        informes = r_ied['values']
                        for inf in informes:

                            tiempo = str(inf['time'])
                            gatewayType = int(inf['gatewayType'])
                            lat = inf['location']
                            lat = str(lat ['lat'])
                            lon = inf['location']
                            lon = str(lon['lon'])
                            alt = inf['location']
                            alt = str(alt['altitude'])
                            location = str(lat + ', ' + lon + ','+ alt)

                            minRSSI = str(inf['minRSSI'])
                            maxRSSI = str(inf['maxRSSI'])
                            averageRSSI = str(inf['averageRSSI'])
                            gpsFixTimestamp = str(inf['gpsFixTimestamp'])
                            engineState = str(inf['engineState'])
                            terminalPowerState = str(inf['terminalPowerState'])
                            cellModemState = str(inf['cellModemState'])
                            cellModemAntennaState = str(inf['cellModemAntennaState'])
                            gpsModemState = str(inf['gpsModemState'])
                            gpsAntennaState = str(inf['gpsAntennaState'])
                            gpsError = str(inf['gpsError'])
                            gpsFirmwareLevelError = str(inf['gpsFirmwareLevelError'])
                            network = str(inf['network'])
                            rssi = str(inf['rssi'])
                            #cursor.execute("INSERT INTO MAQUINAS_INFO_EST_DISP (tiempo,gatewayType,location,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,gpsError,gpsFirmwareLevelError,network,rssi,idmaquina,flag) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tiempo,gatewayType,location,minRSSI,maxRSSI,averageRSSI,gpsFixTimestamp,engineState,terminalPowerState,cellModemState,cellModemAntennaState,gpsModemState,gpsAntennaState,gpsError,gpsFirmwareLevelError,network,rssi,id_maq,flag)
                #informes de las horas de motor
                elif link['rel'] == 'engineHours':
                    r_hdm = requests.get(link['uri'], headers=headers, auth=auth)
                    r_hdm = r_hdm.json()
                    if r_hdm['total'] != 0:
                        horas_motor = r_hdm['values']
                        for hm in horas_motor:
                            value = hm['reading']
                            value = value['valueAsDouble']
                            unite = hm['reading']
                            unite = str(unite['unit'])
                            reportTime = str(hm['reportTime'])
                            #cursor.execute("INSERT INTO MAQUINAS_HORAS_DE_MOTOR (value,unite,reportTime,idmaquina,flag) VALUES (?,?,?,?,?)", value,unite,reportTime,id_maq,flag)
                #informe de las horas de operacion de cada maquina
                elif link['rel'] == 'hoursOfOperation':
                    r_hdo = requests.get(link['uri'], headers=headers, auth=auth)
                    r_hdo = r_hdo.json()
                    if r_hdo['total'] != 0:
                        horas_op = r_hdo['values']
                        for ho in horas_op:
                            startDate = str(ho['startDate'])
                            endDate = str(ho['endDate'])
                            engineState = int(ho['engineState'])
                            #cursor.execute("INSERT INTO MAQUINAS_HORAS_DE_OPERACION (startDate,endDate,engineState,idmaquina,flag) values (?,?,?,?,?)",startDate,endDate,engineState,id_maq,flag)
                #informe de historial de ubiacacion
                elif link['rel'] == 'locationHistory':
                    r_hdu = requests.get(link['uri'], headers=headers, auth=auth)
                    r_hdu = r_hdu.json()
                    if r_hdu['total'] != 0:
                        hist_ubi = r_hdu['values']
                       
                        for hu in hist_ubi:
                            lat = hu['point']
                            lat = str(lat['lat'])
                            lon = hu['point']
                            lon = str(lon['lon'])
                            point = str(lat + ' , '+ lon)
                            eventTimestamp = str(hu['eventTimestamp'])
                            gpsFixTimestamp = str(hu['gpsFixTimestamp'])
                            cursor.execute("INSERT INTO MAQUINAS_HISTORIAL_UBICACION (point,eventTimestamp,gpsFixTimestamp,idmaquina,flag) values (?,?,?,?,?)",point,eventTimestamp,gpsFixTimestamp,id_maq,flag)
            print('Maquina '+ id_maq +' insertada correctamente')




#GENERACION DE REGISTROS ALEATORIOS EN LA TABLAS DE HECHOS
#variables para la generaciom  registros aleatorios
cant = 1000
inicio = datetime(2020, 8, 24)
final =  datetime(2020, 8, 28)
z = '.000Z'
engineStat = [0,1,2,3]
#Consulta con los id de las maquinas
id_m = cursor.execute("Select id from MAQUINAS")
id_m = id_m.fetchall()
idm_list = []
for i in id_m:
    id = i[0]
    idm_list.append(id)
#MAQUINAS_ALERTAS
#SCRIPT NO TERMINADO PORQUE BORRARON DATOS DE MAQUINAS
"""
cursor.execute("SELECT TOP 1 * FROM MAQUINAS_ALERTAS ORDER BY id DESC")
consulta = list(cursor)
id_list = []
seconds = ', Seconds'
bus = 0
tipo = consulta[0][1]

id = int(consulta[0][0])
for i in range(cant):
    id = id + 4
    id_list.append(id)
    print(id)

for id in id_list:
    duration = str(random.randint(1,3900)+ seconds)
    ocurrences = random.randint(1,3)
    enguineHours = str(round(random.uniform(3500,3517),2) + seconds)
    machineLinearTime = random.randint(212813526,212813526)
"""
#MAQUINAS_HORAS_DE_MOTOR
consulta_hdm = cursor.execute("SELECT TOP 1 * FROM MAQUINAS_HORAS_DE_MOTOR ORDER BY id DESC")
consulta_hdm = list(cursor)
promedio = cursor.execute("SELECT AVG(value) as promedio FROM MAQUINAS_HORAS_DE_MOTOR")
promedio = list(promedio)
promedio = round(promedio[0][0],2)
id_list = []
value = consulta_hdm[0][1]
unite = consulta_hdm[0][2]
idm = id_m[0]



for i in range(cant) :
    #MAQUINAS_HORAS_DE_MOTOR
    n = round(random.uniform(0.8,1.3),2)
    valor = round(value + (value * n),2)
    random_date = inicio + timedelta(seconds= int((final - inicio).total_seconds() * random.random()))
    random_date = str(random_date) + z
    idm = random.choice(idm_list)
    cursor.execute("INSERT INTO MAQUINAS_HORAS_DE_MOTOR (value,unite,reportTime,idmaquina,flag) VALUES (?,?,?,?,?)", valor,unite,random_date,idm,1)

    #MAQUINAS_HORAS_DE_OPERACION
    fi = datetime.today()    
    fi = datetime(fi.year,fi.month,fi.day)
    t1 = timedelta(seconds= int( random.randint(1000,10000)))   
    fechai = fi + t1
    fechaf = fi + (t1 + t1)
    fechai =str(fechai) + z
    fechaf = str(fechaf) + z
    eng = random.choice(engineStat)
    #cursor.execute("INSERT INTO MAQUINAS_HORAS_DE_OPERACION (startDate,endDate,engineState,idmaquina,flag) values (?,?,?,?,?)",fechai,fechaf,eng,idm,1) 
conexion.commit()
print("Datos cargados exitosamente...")
cursor.close()
conexion.close()