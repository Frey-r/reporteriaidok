import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pymysql
import dotenv
from openpyxl.styles import Color,NamedStyle, Font, Border, PatternFill, colors
from openpyxl import load_workbook
import os
import time
import re
import math

load_dotenv()

#DATOS DE CONECCIÓN-----------------------------------------------------------------------------------------------------
ip=os.getenv('IP')
puerto=os.getenv('PORT')
user=os.getenv('USER')
password=os.getenv('PASS')
bd=os.getenv('DB')

#RUTAS------------------------------------------------------------------------------------------------------------------
excels = os.getenv('EXCEL_ROUTE')
resultados = os.getenv('RESULT_ROUTE')

#DATA AUXILIAR
url='mysql+pymysql://'+user+':'+password+'@'+ip+':'+puerto+'/'+bd
engine = create_engine(url)

mes=['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septieembre','Octubre','Noviembre','Diciembre']
dMes=[31,28,31,30,31,30,31,31,30,31,30,31]

fondoEsperanza ='5f75d4ee502d22387f7f65ca'		#'FE'
sinacofi = '5edf0961edf41508761849cc'			#'SINACOFI'
bancoInternacional = '5fa54315edf41508348a194c'	#'BANCO INTERNACIONAL'
serviciosFinancieros='5fc9053a502d22761dfaf62a'	#'SERVICIOS FINANCIEROS'
baninter='61531261502d224a1ab95ebc'				#'BANINTER'
fintonic='5fbd1084502d2254f287b739'				#'FINTONIC'
gmf='5ff4cde9502d22091e196b6a'					#'GMF'
forum='62311acdedf4150821ad1be2'                #'FORUM':
maat='6064ffc8edf41513b939ba39'                 #'MAAT'
fitrans='62310c64502d220d858fcaaa'              #'FITRANS'

scotiaSafeSigner = '62f6be46502d220e980fc9a9'   #'Safesigner'

idEmpresas=[fondoEsperanza,sinacofi,bancoInternacional,serviciosFinancieros,fintonic,gmf,forum,maat,baninter,fitrans]
nomEmpresas=['FE','SINACOFI','BANCO INTERNACIONAL','SERVICIOS FINANCIEROS',
             'FINTONIC','GMF','FORUM','MAAT','BANINTER','FITRANS']

#CARGA DE LIBROS BD-----------------------------------------------------------------------------------------------------
def libro_BD():
    #NOMBRES DE TABLAS
    docEmpresas = 'mxml-corp-(2023-07-02).xlsx'
    docPersona = 'mxmls-perso-(2023-07-02).xlsx'
    usuarios = 'users-full-(2023-07-02).xlsx'
    empresas = 'enterprises-(2023-07-02).xlsx'

    #LIBROS DE DATOS
    libroUsuarios = pd.read_excel(excels+usuarios)
    libroDocumentosEmpresa = pd.read_excel(excels+docEmpresas)
    libroDocumentosPersona = pd.read_excel(excels+docPersona)
    libroEmpresas = pd.read_excel(excels+empresas)

    # LIMPIEZA DE FECHAS
    libroDocumentosEmpresa['created_at'] = libroDocumentosEmpresa['created_at'].str[0:10]
    print('Fecha de creación modificada en Documentos')
    libroDocumentosEmpresa['updated_at'] = libroDocumentosEmpresa['updated_at'].str[0:10]
    print('Fecha de actualización modificada en Documentos')

    libroDocumentosPersona['created_at'] = libroDocumentosPersona['created_at'].str[0:10]
    print('Fecha de creación modificada en Documentos')
    libroDocumentosPersona['updated_at'] = libroDocumentosPersona['updated_at'].str[0:10]
    print('Fecha de actualización modificada en Documentos')

    libroUsuarios['updated_at'] = libroUsuarios['updated_at'].str[0:10]
    print('Fecha de actualización modificada en Usuarios\n')

    #CONTEO DE FIRMAS
    libroDocumentosEmpresa['nFirmas'] = libroDocumentosEmpresa['signed_by_ids'].str.count('\\$')
    print('Contando firmas Corp')

    #libroDocumentosEmpresa['fes'] = libroDocumentosEmpresa['fes'].str.count('1')
    #print('Contando fes')

    libroDocumentosPersona['nFirmas'] = libroDocumentosPersona['signed_by_ids'].str.count('\\$')
    print('Contando firmas Naturales\n')

    libroDocumentosEmpresa['documento'] = libroDocumentosEmpresa['nFirmas'].notna().astype(int)
    libroDocumentosPersona['documento'] = libroDocumentosPersona['nFirmas'].notna().astype(int)

    #LIMPIANDO NOMBRES DE COMLUMNA
    libroDocumentosPersona = libroDocumentosPersona.rename(columns={"cloned_template.document_name": "document_name"})
    libroDocumentosEmpresa = libroDocumentosEmpresa.rename(columns={"cloned_template.document_name": "document_name"})

    #.TO_SQL
    libroUsuarios.to_sql(name='usuarios_jun',con=engine,if_exists='replace',index=False)
    print('Libro usuarios cargado')
    libroDocumentosEmpresa.to_sql(name='documentos_empresa_jun',con=engine,if_exists='replace',index=False)
    print('Libro documentos empresa cargado')
    libroDocumentosPersona.to_sql(name='documentos_persona_jun', con=engine, if_exists='replace', index=False)
    print('Libro documentos persona cargado')
    libroEmpresas.to_sql(name='empresas_jun',con=engine,if_exists='replace',index=False)
    print('Libro empresas cargado')

def carga_groups():
    tamano_archivo_dividido = 40000

    chunks = pd.read_csv(excels+'groups_members_count.csv', chunksize=tamano_archivo_dividido)

    contador = 1
    for chunk in chunks:
        nombre_archivo_dividido = f'resultado groups/archivo_dividido_{contador}.csv'
        chunk.to_sql(name='grupos',con=engine,if_exists='replace', index=False)
        contador += 1

def nDocumento():
    #DOCS
    docEmpresas = 'mxml-corp-(2023-05-31).xlsx'
    docPersona = 'mxmls-perso-(2023-05-31).xlsx'

    #LIBROS
    bdEmp='documentos_empresa_may'
    bdPer='documentos_persona_may'

    #LIBROS DE DATOS
    libroDocumentosEmpresa = pd.read_excel(excels+docEmpresas)
    libroDocumentosPersona = pd.read_excel(excels+docPersona)

    #LIMPIANDO NOMBRES DE COMLUMNA
    libroDocumentosPersona = libroDocumentosPersona.rename(columns={"cloned_template.document_name": "document_name"})
    libroDocumentosEmpresa = libroDocumentosEmpresa.rename(columns={"cloned_template.document_name": "document_name"})

    # LIMPIEZA DE FECHAS
    libroDocumentosEmpresa['created_at'] = libroDocumentosEmpresa['created_at'].str[0:10]
    print('Fecha de creación modificada en Documentos')
    libroDocumentosEmpresa['updated_at'] = libroDocumentosEmpresa['updated_at'].str[0:10]
    print('Fecha de actualización modificada en Documentos')

    libroDocumentosPersona['created_at'] = libroDocumentosPersona['created_at'].str[0:10]
    print('Fecha de creación modificada en Documentos')
    libroDocumentosPersona['updated_at'] = libroDocumentosPersona['updated_at'].str[0:10]
    print('Fecha de actualización modificada en Documentos')

    #CONTEO DE FIRMAS
    libroDocumentosEmpresa['nFirmas'] = libroDocumentosEmpresa['signed_by_ids'].str.count('\\$')
    print('Contando firmas Corp')
    libroDocumentosPersona['nFirmas'] = libroDocumentosPersona['signed_by_ids'].str.count('\\$')
    print('Contando firmas Pers')

    #CONTEO DOCS
    libroDocumentosEmpresa['documento'] = libroDocumentosEmpresa['signed_by_ids'].str.len().apply(
        lambda x: 1 if x > 4 else 0)
    libroDocumentosPersona['documento'] = libroDocumentosPersona['signed_by_ids'].str.len().apply(
        lambda x: 1 if x > 4 else 0)


    #TOSQL
    libroDocumentosEmpresa.to_sql(name=bdEmp,con=engine,if_exists='replace',index=False)
    print('Libro documentos empresa cargado')
    libroDocumentosPersona.to_sql(name=bdPer, con=engine, if_exists='replace', index=False)
    print('Libro documentos persona cargado')

#UTILIDADES-------------------------------------------------------------------------------------------------------------
def unir_tablas(tabla1,tabla2):

    queryDocs = f"SELECT * \
                FROM {tabla1} \
                WHERE _id NOT IN (SELECT _id FROM {tabla2}) \
                UNION ALL \
                SELECT * \
                FROM {tabla2};"

    #queryDocs = f"SELECT \
    #            COALESCE(t1._id, t2._id) AS _id, \
    #            COALESCE(t1.mxml_creator_id, t2.mxml_creator_id) AS mxml_creator_id, \
    #            COALESCE(t1.enterprise_id, t2.enterprise_id) AS enterprise_id, \
    #            COALESCE(t1.updated_at, t2.updated_at) AS updated_at, \
    #            COALESCE(t1.created_at, t2.created_at) AS created_at, \
    #            COALESCE(t1.document_name, t2.document_name) AS document_name, \
    #            COALESCE(t1.signed_by_ids, t2.signed_by_ids) AS signed_by_ids, \
    #            COALESCE(t1.fes, t2.fes) AS fes, \
    #            COALESCE(t1.nFirmas, t2.nFirmas) AS nFirmas \
    #            FROM {tabla1} t1 \
    #            LEFT JOIN {tabla2} t2 ON t1._id = t2._id \
    #            WHERE t1._id IS NULL OR t2._id IS NULL \
    #            UNION ALL \
    #            SELECT t1._id, \
    #            t1.mxml_creator_id, \
    #            t1.enterprise_id, \
    #            t1.updated_at, \
    #            t1.created_at, \
    #            t1.document_name, \
    #            t1.signed_by_ids, \
    #            t1.fes,\
    #            t1.nFirmas \
    #            FROM {tabla1} t1 \
    #            LEFT JOIN {tabla2} t2 ON t1._id = t2._id \
    #            WHERE t1.updated_at > t2.updated_at OR t2._id IS NULL;"

    #queryUsers = f"SELECT \
    #            COALESCE(t1._id, t2._id) AS _id, \
    #            COALESCE(t1.rut, t2.rut) AS rut, \
    #            COALESCE(t1.updated_at, t2.updated_at) AS updated_at, \
    #            COALESCE(t1.fullname, t2.fullname) AS fullname \
    #            FROM {tabla1} t1 \
    #            LEFT JOIN {tabla2} t2 ON t1._id = t2._id \
    #            WHERE t1._id IS NULL OR t2._id IS NULL \
    #            UNION ALL \
    #            SELECT t1._id, \
    #            t1.rut, \
    #            t1.updated_at, \
    #            t1.fullname \
    #            FROM {tabla1} t1 \
    #            LEFT JOIN {tabla2} t2 ON t1._id = t2._id \
    #            WHERE t1.updated_at > t2.updated_at OR t2._id IS NULL;"

    queryUsers = f"select * from {tabla1} " \
                 f"union all " \
                 f"select * from {tabla2};"

    df = pd.read_sql_query(queryDocs,con=engine)
    df.to_sql('certs_aux',con=engine,index=False,if_exists='replace')

def libro_planes():
    planes = pd.read_excel(excels+'PLANES.xlsx')
    planes.to_sql(name='planes_jul',con=engine,if_exists='replace',index=False)
    query = 'SELECT p.FYA, p.RUT, p.NEM,p.PLAN,e._id \
            FROM Firma_Ya.planes_jul p\
            join emp_jul e \
            on p.RUT = e.rut;'
    planes = pd.read_sql_query(query,con=engine)
    planes.to_sql(name='planes', con=engine, if_exists='replace', index=False)

def limpiar_Rut():
    df = pd.read_excel(excels+'PLANES (2).xlsx')
    df['RUT'] = df['RUT'].str.replace('.', '')
    df.to_excel('excels/PLANES (2).xlsx',index=False)

#INFORMES---------------------------------------------------------------------------------------------------------------
def informe_general_corp(fechaInicio,fechaFinal,tablaDocs,tablaEmp):
    print('Importando información de la BD...')

    query = "SELECT e._id as 'ID Empresa',e.simple_name as 'Nombre empresa',"\
            "sum(d.documento) as 'Documentos',sum(d.nFirmas) as 'Firmas'"\
            f"FROM {tablaEmp} e " \
            f"left join {tablaDocs} d on e._id = d.enterprise_id " \
            "where d.updated_at between '"+fechaInicio+"' and '"+fechaFinal+"' " \
            "group by e._id,e.simple_name "\
            "order by e._id asc;"

    df = df = pd.read_sql_query(query, con=engine)

    print(df)
    print('Guardando...')
    df.to_excel(resultados+'informe General.xlsx', index=False,sheet_name='Corp')

def informe_general_pers(fechaInicio,fechaFinal):
    print('Importando información de la BD...')

    query = "SELECT u._id as 'ID Usuario', u.rut as 'Rut', " \
            "u.fullname as 'Nombre', sum(d.documento) as 'Documentos', " \
            "sum(d.nFirmas) as 'Firmas '"\
            "FROM users_oct u " \
            "left join documentos_persona_oct d on u._id = d.mxml_creator_id " \
            "where d.updated_at between '"+fechaInicio+"' and '"+fechaFinal+"' " \
            "group by u._id,u.rut,u.fullname order by u._id asc;"

    df = df = pd.read_sql_query(query, con=engine)

    print('Guardando...')
    df.to_excel(resultados+'informe General Persona.xlsx', index=False,sheet_name='Persona')

def corp_planes(fechaInicio,fechaFinal):

    print('Importando información de la BD...')

    #queryTabla='(SELECT p.FYA,p.RUT,p.PLAN,e._id '\
    #            'FROM planes_jun p '\
    #            'join empresas_jul e '\
    #            'on e.simple_name = p.FYA) '\
    #            'union '\
    #            '(SELECT p.FYA,p.RUT,p.PLAN,e._id '\
    #            'FROM planes_jun p '\
    #            'join empresas_jun e '\
    #            'on e.rut = p.RUT '\
    #            'where p.FYA = NULL)'
    #query = "SELECT e._id as 'ID Empresa',e.simple_name as 'Nombre empresa',"\
    #        "count(d._id) as 'Documentos' ,sum(d.nFirmas) as 'Firmas',"\
    #        "sum(d.fes) as FES "\
    #        "FROM emp_plan e " \
    #        "left join documentos_empresa_mar d on e._id = d.enterprise_id " \
    #        "where d.updated_at between '"+fechaInicio+"' and '"+fechaFinal+"' " \
    #        "group by e._id,e.simple_name "
#
    #df = df = pd.read_sql_query(query, con=engine)
    #print('Guardando...')
    #df.to_sql("GENERAL temp",con=engine,index=False)

#    query = "SELECT e.IDEmpresa, e.Nombreempresa, p.plan,"\
#            "e.Documentos,e.Firmas, e.FES,e.FEA "\
#            "FROM `GENERAL_TEMP` e " \
#            "join EMP_TEMP p on p._id = e.IDEmpresa;"
#    dfTabla = pd.read_sql_query(queryTabla,con=engine)
#    dfTabla.to_sql(name='planes_junio',con=engine,if_exists='replace',index=False)

    query=f"SELECT p._id as 'ID Empresa',p.FYA as 'Nombre empresa', p.NEM, p.PLAN as 'plan', \
            sum(d.documento) as 'Documentos',sum(d.nFirmas) as 'Firmas' \
            FROM planes_jul p \
            join documentos_empresa_oct d \
            on p._id = d.enterprise_id  \
            Where updated_at between '{fechaInicio}' and '{fechaFinal}'\
            GROUP BY p._id, p.FYA,p.PLAN,p.NEM;"

    df = pd.read_sql_query(query,con=engine)
    df.to_excel(resultados+'Informe General Plan Agosto.xlsx',index=False)

def rutificado_FE(tabla,tabla_users,fechaInicio,fechaFinal):
    query=f'select _id,rut from {tabla_users}'
    df = pd.read_sql_query(query,con=engine)
    Druts = {}
    patron = re.compile(r'"\$oid"\s*:\s*"(\w{24})"')
    listaIds = []
    listaRuts = []

    count=0
    for i in range(df.shape[0]-1):
        Druts[df.loc[i+1,'_id']]=df.loc[i+1,'rut']

#QUERYS--------------------------------------------------------------
    queryFE = f"Select d._id,d.mxml_creator_id,d.enterprise_id,g.name as 'group_name',g.members_count,  \
        d.updated_at, d.created_at,d.document_name,d.signed_by_ids,d.nFirmas \
        from `{tabla}` d join `grupos` g on d.pelugroup_id = g._id \
        where d.enterprise_id = '5f75d4ee502d22387f7f65ca' \
        and d.updated_at between '{fechaInicio}' and '{fechaFinal}' "

    fe=pd.read_sql_query(queryFE,con=engine)
    cadenaIds = ""
    for i in range(len(fe.index)):
        signers = fe.loc[i,'signed_by_ids']
        try:
            cadenaIds = patron.findall(signers)
        except:
            cadenaIds = ""
        listaIds.append(cadenaIds)
    for ids in listaIds:
        count+=1
        lRuts=""
        for id in ids:
            try:
                rut = Druts[id]
                lRuts = rut+", "+lRuts
            except:
                lRuts = id+", "+lRuts
        listaRuts.append(lRuts)

    final = pd.read_sql_query(queryFE,con=engine)
    firmantes = pd.DataFrame(listaRuts,columns=['signers'])
    finalyfirmantes = pd.concat([final,firmantes],axis=1)
    finalyfirmantes.to_excel(resultados+'fondo esperanza julio.xlsx',sheet_name= 'julio', index=False)

def informe_anual_corp():
    for i in range(9):
        print('importando datos ' + str(i + 1) + '/2021...')


        query = "SELECT d._id as 'ID Documento',u.rut as 'Rut Creador', d.enterprise_id," \
                "d.updated_at as 'Fecha de Actualizacion',d.created_at as 'Fecha de Creacion' ," \
                "d.document_name as 'Nombre de Documento',d.nFirmas 'Numero de Firmas' " \
                "FROM documentos_empresa_2020 d left join usuarios u on u._id = d.mxml_creator_id " \
                "WHERE d.updated_at BETWEEN '2021-0" + str(i + 1) + "-01' and '2021-0" + str(i + 1) + "-" + str(dMes[i]) + \
                "' order by d.updated_at asc;"

        df = pd.read_sql_query(query, con=engine)

        print('Guardando informe...\n')
        df.to_excel(resultados+'CORP ' + str(i + 1) + '-2021.xlsx', index=False)

def informe_esperanza(mesAnterior,mesActual):
    query=  "SELECT a._id,a.mxml_creator_id,a.enterprise_id,a.pelugroup_id,a.updated_at,a.created_at,a.document_name," \
            "a.signed_by_ids,a.nFirmas,e.nFirmas as nFirmas_D, a.nFirmas - e.nFirmas as delta "\
            f"FROM documentos_empresa_{mesActual} a "\
            f"left join documentos_empresa_{mesAnterior} e on e._id = a._id;"
    df = pd.read_sql_query(query,con=engine)
    df.to_sql('FE_TEMP',con=engine,if_exists='replace', index = False)
    query = f"select d._id,d.mxml_creator_id,d.document_name,g.name as 'group_name',d.created_at,d.updated_at,d.signed_by_ids,d.nFirmas \
            from FE_TEMP d \
            JOIN grupos g \
            on d.pelugroup_id = g._id \
            where d.enterprise_id = '5f75d4ee502d22387f7f65ca';"
    df = pd.read_sql_query(query,con=engine)
    df.to_excel(resultados+'fondo esperanza mensual.xlsx',index=False)

def informeCompleto(fechaInicio,fechaFinal):
    print('Importando información de la BD...')
    y=0
    for i in idEmpresas:

        query = "SELECT d._id as 'ID Documento',u.rut as 'Rut Creador', " \
                "d.enterprise_id as 'ID Empresa', d.updated_at as 'Fecha de Actualizacion'," \
                "d.created_at as 'Fecha de Creacion' ,"\
                "d.document_name as 'Nombre de Documento'," \
                "d.pelugroup_id as 'group_id'," \
                "d.nFirmas as 'Numero de Firmas' " \
                "FROM documentos_empresa_oct d " \
                "left join users_oct u on u._id = d.mxml_creator_id " \
                "WHERE d.enterprise_id = '"+i+"' AND  d.updated_at " \
                "between '"+fechaInicio+"' AND '"+fechaFinal+"' " \
                "order by d.updated_at asc;"
        df = pd.read_sql_query(query,con=engine)
        nFilas = len(df.index)

        print('Contando Firmas de '+str(nomEmpresas[y])+'...')

        print('Guardando...')
        df.to_excel(resultados+'informe ' +str(nomEmpresas[y])+ '.xlsx',index=False,sheet_name=nomEmpresas[y])

        y += 1

#INICIALIZACIONES-------------------------------------------------------------------------------------------------------
#el formato de fecha es YYYY-MM-DD
carga_groups()