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
        
        with ElasticSearchProvider() as es:
            response = es.insert_document("1", document)
            print("response: ", response)
        

    except Exception as e:
        print(f"An error occured: {e}")

if __name__ == "__main__":
    main()
