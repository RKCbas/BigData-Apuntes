from ElasticSearchProvider import ElasticSearchProvider
import json

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider()
        print("es_handler: ", es_handler, "\n")

        # Insert a document into the index
        document = {
            "name": "John Doe",
            "first_name": "John",
            "last_name": "Doe",
            "age": 25,
            "ocupation": "Software Developer",
            "salary": 1000,
            "city": "New York",
            "country": "USA"
        }
        
        RESPONSE_LITERAL = "response: "
        
        with ElasticSearchProvider() as es:
            
            # **Show all the indices**
            # response = es.show_all_indices()
            # print("Show All Indices Response:")
            # print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

            # Insert a document into the index
            response = es.insert_document("1", document)
            print("Insert Document Response:")
            print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")

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
            # response = es.delete_document("1")
            # print("Delete Document Response:")
            # print(f"{RESPONSE_LITERAL} {json.dumps(response.body, indent=4)}\n")
        
            

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
