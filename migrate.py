"""Migrando Base MySQL para MongoDB"""

import copy

from pymongo import MongoClient
import pymysql

import config
import logging
from logging.handlers import RotatingFileHandler

RESET_MONGO_COLLECTIONS_ON_UPDATE = True 
PRINT_INFO = True 
PRINT_RESULTS = True 

def initalise_mysql():
    """Inicializando MySQL"""
    return pymysql.connect(
        host=config.MYSQL_HOST,
        user=config.MYSQL_USERNAME,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB)

def initalise_mongo():
    """Inicializando MongoDB"""
    return MongoClient(config.MONGO_HOST, config.MONGO_PORT)[config.MONGO_DB]

def extract_data(mysql_cursor):
    """Given a cursor, Extracts data from MySQL movielens dataset
    and returns all the tables with their data"""
    empresas = execute_mysql_query('select * from empresas', mysql_cursor, 'fetchall')
    funciona = execute_mysql_query('select * from funciona', mysql_cursor, 'fetchall')    
    tables = (empresas, funciona)
    return tables

def execute_mysql_query(sql, cursor, query_type):
    """executando consultas no MySQL"""
    if query_type == "fetchall":
        cursor.execute(sql)
        return cursor.fetchall()
    elif query_type == "fetchone":
        cursor.execute(sql)
        print(sql)
        return cursor.fetchall()
    else:
        pass

def transform_data(dataset, table):
    """Transformando resultado da consulta em JSON"""
    dataset_collection = []
    tmp_collection = {}
    if table == "empresas":
        for item in dataset[0]:
            tmp_collection['cemp'] = item[0]
            tmp_collection['razao_sc'] = item[1]
            dataset_collection.append(copy.copy(tmp_collection))
        return dataset_collection      
    elif table == "funciona":
        for item in dataset[1]:
            tmp_collection['cemp'] = item[0]
            tmp_collection['cmat'] = item[1]
            tmp_collection['nomecomp'] = item[2]
            dataset_collection.append(copy.copy(tmp_collection))
        return dataset_collection

def load_data(mongo_collection, dataset_collection):
    """Carregando dados no MongoDB e retornando o resultado"""
    if RESET_MONGO_COLLECTIONS_ON_UPDATE:
        mongo_collection.delete_many({})
    return mongo_collection.insert_many(dataset_collection)

def main():   
    if PRINT_INFO:
        print('Iniciando Pipeline de Dados')
        print('Inicializando Conexão com MySQL')
    mysql = initalise_mysql()
    
    if PRINT_INFO:
        print('Conexão com o MySQL Completada')
        print('Estágio 1: extraindo dados do MySQL')
    mysql_cursor = mysql.cursor()
    mysql_data = extract_data(mysql_cursor)

    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.info('Iniciando leitura das bases de dados')
    records = mysql_cursor
    logger.info(mysql_data)
    logger.debug('Records: %s', records)
    
    if PRINT_INFO:
        print('Estágio 1: Finalizado! Extração realizada com sucesso da base MySQL')
        print('Iniciando Estágio 2: Transformando os dados da base MySQL para MongoDB')
        print('Preparando para migrar tabela de empresas')
    empresas_collection = transform_data(mysql_data, "empresas")
    
    if PRINT_INFO:
        print('Migração concluída da tabela de Empresas')
        print('Preparando para migrar tabela de Funcionarios')
    funciona_collection = transform_data(mysql_data, "funciona")
    
    if PRINT_INFO:
        print('Migração concluída da tabela de Funcionarios')
        print('Estágio 2: Finalizado!  Dados carregados com sucesso')
        print('Inicializando processo de conexão com o MongoDB')
    mongo = initalise_mongo()
    
    if PRINT_INFO:
        print('Conexão com o MongoDB completa')
        print('Estágio 3: Preparando dados para o MongoDB')
    result = load_data(mongo['empresas'], empresas_collection)
    
    if PRINT_RESULTS:
        print('Carregando collection de empresas')
        print(result)
    result = load_data(mongo['funciona'], funciona_collection)
    
    if PRINT_RESULTS:
        print('Carregando collection de funcionarios')
        print(result)
    
    if PRINT_INFO:
        print('Estágio 3: Finalizado! Dados migrados com sucesso!')
        print('Fechando conexão com o MySQL')
    mysql.close()
    if PRINT_INFO:        
        print('encerrando migração')

if __name__ == "__main__":
    main()
