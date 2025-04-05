import json, time, pandas as pd
from elasticsearch import *
from elasticsearch import helpers
from elasticsearch import Elasticsearch, ConnectionError

class ElasticSearchProvider:
    
    def __init__(self, index="person"):
        self.host = "http://localhost:9200"
        #   self.user = str(user)
        #   self.password = str(password)
        self.index = index
        self.index_type = "_doc"
        # Configurar timeout, re-intentos y retry_on_timeout
        self.connection = Elasticsearch(
            self.host,
            timeout=30,  # Tiempo de espera en segundos
            max_retries=3,  # Número máximo de re-intentos
            retry_on_timeout=True  # Re-intentar si hay un timeout
        )

    def __enter__(self):
        try:
            self.connection = Elasticsearch(
                self.host,
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            return self
        except ConnectionError as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": f"Connection error: {str(e)}"
                })
            }
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def __exit__(self, exception_type, exception_val, exception_traceback):
        self.connection.close()
    
    def create_index(self, mapping):
        try:
            if not self.connection.indices.exists(index=self.index):
                response = self.connection.indices.create(index=self.index, body=mapping)
            else:
                response = {
                    "StatusCode": 400,
                    "body": json.dumps({
                        "message": f"Index {self.index} already exists"
                    })
                }
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
        
    def get_mapping(self):
        try:
            response = self.connection.indices.get_mapping(index=self.index)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }

    def insert_document(self, doc_id, document):
        try:
            response = self.connection.index(index=self.index, id=doc_id, body=document)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
        
    def get_document(self, doc_id):
        try:
            response = self.connection.get(index=self.index, id=doc_id)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def get_all_documents(self):
        try:
            response = self.connection.search(index=self.index, body={"query": {"match_all": {}}})
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }

    def search_document(self, query):
        try:
            response = self.connection.search(index=self.index, body=query)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def update_document(self, doc_id, document):
        try:
            response = self.connection.update(index=self.index, id=doc_id, body=document)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def update_document_by_query(self, query, script):
        try:
            response = self.connection.update_by_query(
                index=self.index, 
                body={
                    "query" : query, 
                    "script" : script
                    }, 
                conflicts='proceed')
            time.sleep(1)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
        
    def bulk_update_documents(self, firstname, lastname, updated_fields):
        try:
            query = {
                "script": {
                    "source": "; ".join([f"ctx._source.{field} = params['{field}']" for field in updated_fields]),
                    "params": updated_fields
                },
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "firstname": firstname
                                }
                            },
                            {
                                "match": {
                                    "lastname": lastname
                                }
                            }
                        ]
                    }
                }
            }
            response = self.connection.update_by_query(index=self.index, body=query, conflicts='proceed')
            time.sleep(1)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }

    def delete_document(self, doc_id):
        try:
            response = self.connection.delete(index=self.index, id=doc_id)  
            time.sleep(1)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
        
    def delete_all_documents(self):
        try:
            response = self.connection.delete_by_query(index=self.index, body={"query": {"match_all": {}}})
            time.sleep(1)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
        
    def bulk_delete_documents(self, firstname, lastname):
        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "firstname": firstname
                                }
                            },
                            {
                                "match": {
                                    "lastname": lastname
                                }
                            }
                        ]
                    }
                }
            }
            response = self.connection.delete_by_query(index=self.index, body=query)
            time.sleep(1)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def delete_index(self):
        try:
            response = self.connection.indices.delete(index=self.index)
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def show_all_indices(self):
        try:
            response = self.connection.indices.get_alias(index="*")
            if not response:
                return {
                    "StatusCode": 404,
                    "body": json.dumps({
                        "message": "No indices found"
                    })
                }
            return response
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": str(e)
                    })
            }
    
    def load_json_file(self, file_path, batch_size=1000):
        try:
            # Leer el archivo JSON usando pandas
            df = pd.read_json(file_path, orient="records", lines=False)

            # Convertir el DataFrame a una lista de diccionarios
            documents = df.to_dict(orient="records")
            
            # Dividir los documentos en lotes
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                
                # Preparar los datos para la operación bulk
                bulk_data = [
                    {"_index": self.index, "_source": doc}
                    for doc in batch
                ]
                
                # Insertar los documentos en Elasticsearch
                helpers.bulk(self.connection, bulk_data)
            
            return f"{len(documents)} documents inserted in {self.index}"
        
        except ValueError as e:
            return {
                "StatusCode": 400,
                "body": json.dumps({
                    "message": f"Error reading JSON with pandas: {str(e)}"
                })
            }
        except FileNotFoundError as e:
            return {
                "StatusCode": 404,
                "body": json.dumps({
                    "message": f"File not found: {str(e)}"
                })
            }
        except ConnectionError as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": f"Connection error: {str(e)}"
                })
            }
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": f"An error occurred: {str(e)}"
                })
            }