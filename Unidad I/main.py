from ElasticSearchProvider import ElasticSearchProvider
import json, time

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider()
        print("es_handler: ", es_handler, "\n")

        # Test documents
        document1 = {
            "name": "John Doe",
            "first_name": "John",
            "last_name": "Doe",
            "age": 25,
            "ocupation": "Software Developer",
            "salary": 1000,
            "city": "New York",
            "country": "USA"
        }

        document2 = {
            "name": "Jane Smith",
            "first_name": "Jane",
            "last_name": "Smith",
            "age": 30,
            "ocupation": "Data Scientist",
            "salary": 1200,
            "city": "San Francisco",
            "country": "USA"
        }

        document3 = {
            "name": "Alice Johnson",
            "first_name": "Alice",
            "last_name": "Johnson",
            "age": 28,
            "ocupation": "Product Manager",
            "salary": 1100,
            "city": "Seattle",
            "country": "USA"
        }

        document4 = {
            "name": "Bob Brown",
            "first_name": "Bob",
            "last_name": "Brown",
            "age": 35,
            "ocupation": "DevOps Engineer",
            "salary": 1300,
            "city": "Austin",
            "country": "USA"
        }
        
        RESPONSE_LITERAL = "response: "
        
        with ElasticSearchProvider() as es:
            
            # **Show all the indices**
            # response = es.show_all_indices()
            # print("Show All Indices Response:")
            # print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Insert a document into the index
            print("Insert Document Response:")
            response = es.insert_document("1", document1)            
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.insert_document("1", document2)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.insert_document("1", document3)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.insert_document("1", document4)
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            time.sleep(1)

            # Get the document by its id
            response = es.get_document("1")
            print("Get Document Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Update the document
            response = es.update_document("1", {
                "doc": {
                    "age": 26
                }
            })
            print("Update Document Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Update the document by a query
            response = es.update_document_by_query({
                "match": {
                    "name": "John Doe"
                }
            }, {
                "source": "ctx._source.age += 1"
            })

            # Update the document by a query
            response = es.update_document_by_query({
                #query
                "match": {
                    "name": "Jane Smith"
                }
            }, {
                #script
                "source": "ctx._source[age] += 1"
            })

            # Bulk update documents by a query
            response = es.bulk_update_documents("John", "Doe", 
                {"age": 30,
                "salary": 1500}
            )
            print("Bulk Update Documents Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            

            # Search for a document by a specific field
            response = es.search_document({
                "query": {
                    "match": {
                        "name": "John Doe"
                    }
                }
            })
            print("Search Document by Field Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # gt: greater than
            # gte: greater than or equal
            # lt: less than
            # lte: less than or equal
            # Search for a document by a range of values
            response = es.search_document({
                "query": {
                    "range": {
                        "age": {
                            "gte": 25
                        }
                    }
                }
            })
            print("Search Document by Range Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Search for all documents in the index
            response = es.search_document({
                "query": {
                    "match_all": {}
                }
            })
            print("Search All Documents Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Delete a document by its id
            print("Delete Document Response:")
            response = es.delete_document("1")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.delete_document("2")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.delete_document("3")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            response = es.delete_document("4")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
            

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
