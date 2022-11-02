
from email.mime import base
import os
import csv
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from modules.dbConnect import DBEngine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import INTEGER, CHAR, VARCHAR 
app = Flask(__name__)

endPoint = "/archive"

BASE_PATH = os.getcwd()
UPLOAD_FOLDER = BASE_PATH + "/Darchives/"
ALLOWED_EXTENSIONS = {'csv'}

if not os.path.exists(UPLOAD_FOLDER):
  mode = 0o755
  os.mkdir(UPLOAD_FOLDER, mode)

@app.route(endPoint + '/testcsv', methods = ['POST'])
def receiveCVS():

  DBcon = DBEngine('mariadb', 'padron')

  existingTables = DBcon.getTableNames()
  
  if len(request.form) == 1 and 'name' in request.form.keys():
    v = request.form
    vName = v['name'].lower()
    
    if vName != '':
      if vName.isalpha():
        futureTableName = "padron_" + str(vName)
        
        if futureTableName not in existingTables:
          # DBcon.createDinamicTable(futureTableName)
          rF = request.files

          if len(rF) == 1 and 'file' in rF.keys():
            fileToRead = rF['file']
            
            if fileToRead.mimetype == "text/csv":
              secureFileName = secure_filename(fileToRead.filename)

              fileToRead.save(UPLOAD_FOLDER + secureFileName)
              f = open(UPLOAD_FOLDER + secureFileName)
              reader = csv.reader(f, delimiter= ';')

              segmentosUtilizables = DBcon.getRegisters('segmentos')

              segUt = {}

              for seg in segmentosUtilizables:
                # segUt["M"] = {}
                if not isinstance(segUt.get(seg.genero), dict): segUt[seg.genero] = {}

                segUt[seg.genero][seg.rango] = seg.id

              # segUt["M"]["16 a 29"] = 1
              # segUt["M"]["30 a 49"] = 2
              # ...
              # segUt["F"]["50 a 64"] = 7
              # segUt["F"]["+65"] = 8

              insertsOK = 0
              insertsNotOK = 0
              insertsErrors = {}
              for row in reader:
                if row[1] in segUt.keys() and row[2] in segUt[row[1]]:
                  salida = DBcon.addRegistersD(futureTableName, {'dni': row[0], 'icv': row[3], 'mesa': row[4], 'segmento': segUt[row[1]][row[2]]})

                  if salida == "Registro agregado con éxito!!!": insertsOK += 1
                  else:
                    insertsNotOK += 1
                    insertsErrors[row[0]] = salida
                else:
                  insertsNotOK += 1
                  insertsErrors[row[0]] = "No se encontro segmento!!"
              
              os.remove(UPLOAD_FOLDER + secureFileName)

              fileToRead.close()
              f.close()
              
              return jsonify({"insertados" : insertsOK, "noInsertados" : insertsNotOK, "insertsErrors" : insertsErrors}), 200
            return jsonify("Solo se permiten archivo con extensión .csv"), 400
          return jsonify("El único campo que debe acompañar al archivo es 'file' en el form"), 400
        return jsonify("Ya existe una tabla para el padron con ese nombre"), 400  
      return jsonify("Campo 'name' solo puede contener caracteres alfabéticos form"), 400
    return jsonify("Campo 'name' vacio en el form"), 400
  return jsonify("El único campo aceptado es 'name' en el form"), 400

    # Antes de todo, tenés que controlar que te mande un nombre para la tabla.
    # Segundo que no esté vacío y sea un nombre válido.
    # Tercero que lo que te mando como file no esté vacío.
    # Cuarto, que sea CSV.
    # Fijate que Python debe de tener un módulo para leer CSV's ...
    # Recordá que los valores, en el CSV, están separados por ";" (punto y coma)

  