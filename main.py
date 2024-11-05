from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
import ssl

context = ssl.create_default_context(cafile="V:\elasticsearch-8.15.3\config\certs\http_ca.crt")
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=("elastic", "AmHkHvM3gf_TgCmzk-7r"),  # Replace with your actual username and password
    ssl_context=context
)


# Load dataset into pandas
df = pd.read_csv("Employee_Data.csv", encoding='ISO-8859-1')
# print(df)

# a) Create Collection
def createCollection(p_collection_name):
    p_collection_name = p_collection_name.lower()
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection {p_collection_name} created.")
    else:
        print(f"Collection {p_collection_name} already exists.")


def indexData(p_collection_name, data_type):
    # Make sure the collection name is lowercase
    p_collection_name = p_collection_name.lower()
    
    # Read the data from your source (assuming CSV, DataFrame, or similar)
    data = pd.read_csv('Employee_Data.csv', encoding='ISO-8859-1')  # Adjust as needed
    
    # Replace NaN values with None (JSON-compatible)
    data = data.replace({np.nan: None})
    
    # Loop through each row and index it in Elasticsearch
    for _, row in data.iterrows():
        try:
            # Convert row to dictionary, handling NaN values
            es.index(index=p_collection_name, document=row.to_dict())
        except Exception:
            print(f"Failed to index document: {Exception}")
    
    print(f"Data have been Indexed Successfully in the {p_collection_name} indices")


# c) Search by Column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    p_collection_name = p_collection_name.lower()
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    return result['hits']['hits']

# d) Get Employee Count
def getEmpCount(p_collection_name):
    # Convert the index name to lowercase
    p_collection_name = p_collection_name.lower()
    
    try:
        # Get the count of documents in the specified index
        count = es.count(index=p_collection_name)['count']
        print(f"Document count in '{p_collection_name}': {count}")
    except:
        print(f"Index '{p_collection_name}' not found.")

def delCollection(p_collection_name):
    p_collection_name = p_collection_name.lower()

    if es.indices.exists(index=p_collection_name):
        try:
            es.indices.delete(index=p_collection_name)
            print(f"Deleted successfully.")
        except:
            print(f"Fai. Document not found.")

    
def delEmpById(p_collection_name, p_employee_id):
    # Make sure the collection name is lowercase
    p_collection_name = p_collection_name.lower()
    
    # Search for the document using "Employee ID" field in the _source
    search_body = {
        "query": {
            "term": {
                "Employee ID.keyword": p_employee_id
            }
        }
    }
    
    # Execute the search query
    search_response = es.search(index=p_collection_name, body=search_body)
    
    # Check if any document was found
    if search_response['hits']['total']['value'] > 0:
        # Get the actual _id of the document to delete
        doc_id = search_response['hits']['hits'][0]['_id']
        
        # Proceed to delete the document using the _id
        try:
            es.delete(index=p_collection_name, id=doc_id)
            print(f"Document with Employee ID {p_employee_id} deleted successfully.")
        except Exception as e:
            print(f"Failed to delete document with Employee ID {p_employee_id}. Error: {e}")
    else:
        print(f"Document with Employee ID {p_employee_id} does not exist.")


# f) Get Department Facet
def getDepFacet(p_collection_name):
    p_collection_name = p_collection_name.lower()
    query = {
        "size": 0,
        "aggs": {
            "department_counts": {
                "terms": {"field": "Department.keyword"}
            }
        }
    }
    result = es.search(index=p_collection_name, body=query)
    dep_counts = result['aggregations']['department_counts']['buckets']
    return dep_counts

v_nameCollection = "Anandyukesh"
v_phoneCollection = "0362" 



# c) createCollection(v_nameCollection)
createCollection(v_nameCollection)

# d) createCollection(v_phoneCollection)
createCollection(v_phoneCollection)

# e) Get Employee Count
getEmpCount(v_nameCollection)

# delCollection(v_nameCollection)
delCollection(v_phoneCollection)

# # f) Index Data in v_nameCollection excluding 'Department'
indexData(v_nameCollection, 'Department')

# # g) Index Data in v_phoneCollection excluding 'Gender'
indexData(v_phoneCollection, 'Gender')

# # h) Get Employee Count after Indexing
getEmpCount(v_nameCollection)

# # i) Delete Employee by ID
delEmpById(v_nameCollection, 'E02003')

# # j) Get Employee Count after Deletion
getEmpCount(v_nameCollection)

# # k) Search by Column
print(searchByColumn(v_nameCollection, 'Department', 'IT'))

# # l) Search by Column
print(searchByColumn(v_nameCollection, 'Gender', 'Male'))

# # m) Search in v_phoneCollection
print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

# # n) Get Department Facet for v_nameCollection
print(getDepFacet(v_nameCollection))

# # o) Get Department Facet for v_phoneCollection
print(getDepFacet(v_phoneCollection))

