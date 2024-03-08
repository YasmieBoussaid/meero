import re
import chardet

from config import BUCKET_NAME
from utils import get_city_by_zip, get_zip_by_city

def format_customer_csv_data(row_data:str):
    """
        This functions takes a csv customer data line, extracts the fields and creates a dict for each line of data
        the pattern here is used to ignore splitting data by comma if the comma is inside a string (which is the case
        in some addresses)
        input:
            row_data : str => string value representing a line from a customer csv file
        return:
            dict_data : dict => customer data in dict format
    """
    pattern = re.compile(r',(?=(?:[^"]*"[^"]*")*[^"]*$)')
    data  = re.split(pattern, row_data)
    data = [item.strip() for item in data]
    if len(data) != 5:
        print(row_data)
    else:
        dict_data = {
            "id" : data[0],
            "adress" : data[1],
            "created_at" : data[4]
        }
        try:
            if int(data[3]):
                dict_data['zip'] = int(data[3])
                if data[2] == '':
                    # extract city name
                    dict_data['city'] = get_city_by_zip(data[3])
                else:
                   dict_data['city'] = data[2] 
                
        except:
            if len(data[2]) > 0:
                dict_data['city'] = data[2]
                dict_data['zip'] = get_zip_by_city(data[2])
            else:
                dict_data['city'] = None
                dict_data['zip'] = None
        finally:  
            return dict_data
    return dict_data
    
def get_customer_raw_data(minioClient : object):
    """
        This function extracts the csv customer files stored in MinIO. each file is returned in String format.
        Input : 
            minioClient : Minio object => MinIO client object used to authenticate and retrieve data from MinIO
        Return:

    """
    raw_customer_data = []
    # Get list of CSV objects from the companies bucket
    daily_customer_metadata = minioClient.list_objects(BUCKET_NAME)
    #For each object, get the csv file content and decode it
    for single_file_metadata in daily_customer_metadata:
        object_name = single_file_metadata.object_name
        try:
            response = minioClient.get_object("companies", object_name)
            raw_data = response.read()
            raw_customer_data.append(raw_data)

        finally:
            # Close connection
            response.close()
            response.release_conn()

    return raw_customer_data


def customer_data_cleaning(raw_customer_data : list):
    """
    """
    all_customers = []
    for raw_data in raw_customer_data:
        # decode th data
        detected_encoding = chardet.detect(raw_data)
        csv_content = raw_data.decode(detected_encoding["encoding"])

        #delete header line
        csv_content = csv_content.split('\n',1)[1]
        # replace all newlines with space (this because some addresses may contain new lines instead of space)
        csv_content = csv_content.replace('\n', ' ')
        # regex to get each line ending with a date. Split the csv string file to extract each customer line
        date_pattern = re.compile(r'(.*?\d{4}-\d{2}-\d{2})')
        csv_content_by_line=re.findall(date_pattern,csv_content)

        # format every customer string to a dict object
        customer_dicts = list(map(format_customer_csv_data, csv_content_by_line))

        #add to main list
        all_customers.extend(customer_dicts)
        
    return all_customers