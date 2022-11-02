
from sqlalchemy import create_engine, MetaData, Column, ForeignKey, text, Table
from sqlalchemy.orm import sessionmaker, clear_mappers, Mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER, CHAR, VARCHAR
import os
from dotenv import load_dotenv
from sqlalchemy import inspect

Base = declarative_base()

class ModelSegmentos(Base):
    __tablename__ = 'segmentos'

    id = Column(INTEGER(unsigned=True), primary_key =True)
    genero = Column(CHAR(length=1), nullable=False)
    rango = Column(VARCHAR(length=16), nullable=False)

class DinamicTable(Base):

    __abstract__ = True

    id = Column(INTEGER(unsigned=True), primary_key=True)
    dni = Column(VARCHAR(16), nullable=False)
    icv = Column(VARCHAR(16), nullable=False)
    mesa = Column(VARCHAR(16), nullable=False)
    segmento = Column(INTEGER(unsigned = True), nullable=False, default=1)
        

# class ModelPadron(Base):
#      __tablename__ = 'padron_asd'

    #  id = Column(INTEGER(unsigned=True), primary_key=True)
    #  dni = Column(VARCHAR(16), nullable=False)
    #  icv = Column(VARCHAR(16), nullable=False)
    #  mesa = Column(VARCHAR(16), nullable=False)
    #  segmento = Column(INTEGER(unsigned = True), ForeignKey("segmentos.id"), nullable=False, default=1)

#Acá trato de definir la tabla de una manera en 
#la que pueda ajustar sus parametros en función de algo antes de crearla
# ver cómo sacar los campos del archivo .csv
# defTabla = {'__tablaname__': 'padron_'+'', #Insertar nombre x aparte como un campo"
#             'id': Column(INTEGER(unsigned=True), primary_key =True),
#             'documento': Column(INTEGER(unsigned=True), nullable=False),
#             'segmento': Column(INTEGER(unsigned=True), nullable=False),
#             'icv': Column(INTEGER(unsigned=True), nullable=False),
#             'mesa': Column(VARCHAR(length=16), nullable=False)}

# DinamicTableClass = type('DinamicTableClass', (Base,), defTabla)

load_dotenv()

class DBEngine():
    def __init__ (self, engine, useDB):
        self.engine = engine
        self.user = os.getenv(self.engine + '_USER')
        self.pwd  = os.getenv(self.engine + '_PASS')
        self.host = os.getenv(self.engine + '_HOST')
        self.port = os.getenv(self.engine + '_PORT')
        self.useDB = useDB

        self.defineEngine()

        Base.metadata.create_all(self.sAlq)

        self.connectDB()

        # self.createFirstRegisters()

    def defineEngine(self):
        strConnector  = self.engine + '://' + str(self.user) + ':' + str(self.pwd)
        strConnector += '@' + str(self.host) + ':' + str(self.port) + '/' + self.useDB

        self.sAlq = create_engine(strConnector)

    def connectDB(self):
        # Create a session
        self.__Session = sessionmaker()
        self.__Session.configure(bind=self.sAlq)
        self.__Session = self.__Session()

    def disconnectDB(self):
        # Close session
        self.__Session = self.__Session.close()

    def getTableNames(self):
        insp = inspect(self.sAlq)
        existingTables = insp.get_table_names()

        return existingTables
    
    def getRegisters(self, model, value = None, field = None):
        useModel = self.getModel(model)

        if field:
            return self.__Session.query(useModel).filter(text(field + " = :value")).params(value = value).all()
        elif not value:
            return self.__Session.query(useModel).all()

        return self.__Session.get(useModel, value)

    def getModel(self, model):
        useModel = None

        if model == "segmentos":
            useModel = ModelSegmentos
        elif model == "padron":
            useModel = ModelPadron
        
        return useModel

    def addRegisters(self, model, values):
        # vKeys = values.keys()
        newRegister = self.getModel(model)()

        if isinstance(newRegister, ModelPadron):
            newRegister.dni = values['dni']
            newRegister.icv = values['icv']
            newRegister.mesa = values['mesa']
            newRegister.segmento = values['segmento']

        if newRegister:
            self.__Session.add(newRegister)
            self.__Session.commit()

            return "Registro agregado con éxito!!!"

        return "Algo salió mal!!!"

    def getModelD(self, tabName):
        DynamicBase = declarative_base(class_registry=dict)

        futureTableName = str(tabName)

        class MyModel(DynamicBase):
            tablename = futureTableName.format(tabName=tabName)
            id = Column(INTEGER(unsigned=True), primary_key=True)
            dni = Column(VARCHAR(16), nullable=False)
            icv = Column(VARCHAR(16), nullable=False)
            mesa = Column(VARCHAR(16), nullable=False)
            segmento = Column(INTEGER(unsigned = True), nullable=False, default=1)
        # class_name = futureTableName
        # Model = type(class_name, (DinamicTable,), {
        #     '__tablename__': tablename
        # })


    
        # table_name = futureTableName
        # table_object = Table(futureTableName, metadata,
        #     Column('id', INTEGER(unsigned=True), primary_key=True),
        #     Column('dni', VARCHAR(16), nullable=False),
        #     Column('icv', VARCHAR(16), nullable=False),
        #     Column('mesa', VARCHAR(16), nullable=False),
        #     Column('segmento', INTEGER(unsigned = True), ForeignKey("segmentos.id"), nullable=False, default=1)
        # )

        return MyModel
        
        # Base.metadata.create_table(self.sAlq)

    def addRegistersD(self, model, values):

        XModel = self.getModelD(model)

        XModel.dni = values['dni']
        XModel.icv = values['icv']
        XModel.mesa = values['mesa']
        XModel.segmento = values['segmento']

        self.__Session.add(XModel)
        self.__Session.commit()


    # def inspectDB(self):
    #     inspect(self.__sAlq)
    #     inspect(self.__sAlq).get_table_names()

    # def createFirstRegisters(self):
    #     hasFirstID = self.getRegisters("Segmentos", 1)

    #     if hasFirstID == None:

    #         # genre = ["M", "F"]
    #         # range = ["16 a 29", "30 a 49", " 50 a 64", "+65"]

    #         #Acá trato de incorporar los registros a la tabla

    #         self.__Session.add_all([
    #             modelSegmentos(genero = "M", rango = "16 a 29"),
    #             modelSegmentos(genero = "M", rango = "30 a 49"),
    #             modelSegmentos(genero = "M", rango = "50 a 64"),
    #             modelSegmentos(genero = "M", rango = ">65"),
    #             modelSegmentos(genero = "F", rango = "16 a 29"),
    #             modelSegmentos(genero = "F", rango = "30 a 49"),
    #             modelSegmentos(genero = "F", rango = "50 a 64"),
    #             modelSegmentos(genero = "F", rango = ">65")]
    #             )

    #         self.__Session.commit()

    # def getRegisters(self, model, value = None, field = None):
    #     useModel = self.getModel(model)

    #     if field:
    #         return self.__Session.query(useModel).filter(text(field + " = :value")).params(value = value)
    #     elif not value:
    #         return self.__Session.query(useModel).all()

    #     return self.__Session.get(useModel, value)

    # def getModel(self, model):
    #     useModel = None

    #     if model == "segmentos":
    #         useModel = modelSegmentos
    #     elif model == "dinamic":
    #         useModel = DinamicTableClass
    #     return useModel
    

#Cómo recibo los registros en csv y los meto en la tabla correspondiente?
#Tendría que crear un "if" en el que, sacando la variable nombre "x" del csv meta a todos los registros con ese nombre "x" en la tabla que se va a generar con ese nombre "x"?
#Cuando en el campo "segmento" tenga que comparar los valores de rango y género con los de la tabla "Segmentos"
#Cómo comparo y coloco en el registro de tabla:"padron_"nombre x""" el id del registro de tabla:"Segmento" en que coincidan los campos de rango y género

#Buscar en google como recibir archivos api python.
# SELECT id FROM segmentos WHERE genero = ? AND rango = ?
#idParaF1 = asidfanif
#if rango = ? and genero = ?
#use idParaF1

# valores = SELECT * FROM segmentos
# for val in valores:
#   segmentos[val['genero']][val['rango']] = val['id']
#
# segmentos['M']['16 a 29'] = 1

# if segmentos[csv['genero']][csv['rango']]:
#   segmentoFinal = segmentos[csv['genero']][csv['rango']]