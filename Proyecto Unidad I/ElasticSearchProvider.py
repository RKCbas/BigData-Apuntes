import json, time
from elasticsearch import *
from elasticsearch import helpers

class ElasticSearchProvider:
    
    def __init__(self, index="person"):
        self.host = "http://localhost:9200"
        #   self.user = str(user)
        #   self.password = str(password)
        self.index = index
        self.index_type = "_doc"
        self.connection = Elasticsearch(self.host)

    def __enter__(self):
        try:
            self.connection = Elasticsearch(self.host)
            return self
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
    
    def load_json_file(self, file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                documents = json.load(file)
                if isinstance(documents, dict):
                    documents = [documents]
                bulk_data = [
                    {"_index": self.index, "_source": doc}
                    for doc in documents
                ]
                helpers.bulk(self.connection, bulk_data)
                return f"{len(bulk_data)} documents inserted in {self.index}"
        except json.JSONDecodeError as e:
            return {
                "StatusCode": 400,
                "body": json.dumps({
                    "message": f"Error decoding JSON: {str(e)}"
                })
            }
        except FileNotFoundError as e:
            return {
                "StatusCode": 404,
                "body": json.dumps({
                    "message": f"File not found: {str(e)}"
                })
            }
        except Exception as e:
            return {
                "StatusCode": 500,
                "body": json.dumps({
                    "message": f"An error occurred: {str(e)}"
                })
            }

