from ElasticSearchProvider import ElasticSearchProvider

def main():
    try:
        # Create an instance of the ElasticSearchProvider class
        # and establish a connection with the ElasticSearch server
        es_handler = ElasticSearchProvider()
        print("es_handler: ", es_handler)

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
            # Insert a document into the index
            response = es.insert_document("1", document)
            print(RESPONSE_LITERAL, response)

            # Get the document by its id
            response = es.get_document("1")
            print(RESPONSE_LITERAL, response)

            # Update the document
            response = es.update_document("1", {
                "doc": {
                    "age": 26
                }
            })
            print(RESPONSE_LITERAL, response)

            # Search for a document by a specific field
            response = es.search_document({
                "query": {
                    "match": {
                        "name": "John Doe"
                    }
                }
            })
            print(RESPONSE_LITERAL, response)

            # Search for a document by a range of values
            # gt: greater than
            # gte: greater than or equal
            # lt: less than
            # lte: less than or equal
            response = es.search_document({
                "query": {
                    "range": {
                        "age": {
                            "gte": 25
                        }
                    }
                }
            })
            print(RESPONSE_LITERAL, response)

            # Search for all documents in the index
            response = es.search_document({
                "query": {
                    "match_all": {}
                }
            })
            print(RESPONSE_LITERAL, response)

            # Delete a document by its id
            response = es.delete_document("1")
            print(RESPONSE_LITERAL, response)
        

    except Exception as e:
        print(f"An error occured: {e}")

if __name__ == "__main__":
    main()
