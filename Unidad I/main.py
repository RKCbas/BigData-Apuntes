from ElasticSearchProvider import ElasticSearchProvider

def main():
    try:
        es_handler = ElasticSearchProvider()
        print("es_handler: ", es_handler)
    except Exception as e:
        print(f"An error occured: {e}")

if __name__ == "__main__":
    main()
