import json
from elasticsearch import *

class ElasticSearchProvider:
    
    def __init__(self):
        self.host = "http://localhost:9200"
        #   self.user = str(user)
        #   self.password = str(password)
        self.index = "person"
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
    
    def delete_document(self, doc_id):
        try:
            response = self.connection.delete(index=self.index, id=doc_id)  
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