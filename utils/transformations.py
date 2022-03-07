import json
import pandas as pd
import re
import warnings

# Evaluates the JSON file, removing the fields that aren't desired and evaluating
# if the Property is empty after the removal
# Arguments:
# object - JSON Object which user wants to evaluate
# undesired_properties - List of properties to remove from the JSON Object
# Returns the evaluated JSON Object

def evaluate_json(object, undesired_properties): 
    for value in undesired_properties:
        if value in object: object.pop(value)
    
    return object
    

# Convert Dataframes Columns into a specific types
# Arguments:
# dataframe - The pandas dataframe which wants to be converted
# conversion_table - dictionary where the key is the column name and the value is the type which user wants to convert the key
# Returns the Converted Dataframe

def convert_dataframe(dataframe,conversion_table):
    for key, value in conversion_table.items():
        if 'int' in value or 'float' in value:
            dataframe[key] = pd.to_numeric(dataframe[key], errors='coerce').fillna(0).astype(value)
        if value == 'datetime':
            dataframe[key] = pd.to_datetime(dataframe[key], errors='coerce', infer_datetime_format=True)
        if value == 'string':
            dataframe[key] = dataframe[key].to_string()

    return dataframe 

# Converts a JSON object into a Pandas Dataframe
#
# Arguments:
# json_object - The JSON Object that user wants to convert
# reset_index - Boolean that defines wether reset current dataframe indexes or not
# transpose - Boolean that defines if the returned dataframe will be the transposed version of the original
#
# Returns the Converted Dataframe

def json_to_dataframe(json_object, reset_index=False, transpose=False):
    dataframe = pd.DataFrame(json_object)
    if(transpose == True):
        dataframe = dataframe.T
    if(reset_index == True):
        dataframe.reset_index(inplace=True)
    
    return dataframe

# Rename Dataframe columns based on a conversion table. Checks for duplicate column names
#
# Arguments:
# dataframe - The pandas dataframe which wants to be converted
# conversion_table - Dictionary with the "key" being the old column name and the "value" being 
# the new value
#
# Returns the Renamed Dataframe if it doesn't contains duplicate named columns. Otherwise Raise an Exception. 

def rename_column(dataframe, conversion_table):
    for current_name, new_name in conversion_table.items():
        if current_name in dataframe.columns:
            if new_name in dataframe.columns:
                raise Exception("There's already a column named {0} in the evaluated dataframe".format(new_name))
            dataframe = dataframe.rename(columns={current_name: new_name})
        ## In future, add a warning reporting that the dataframe was not renamed
    return dataframe

# Sanitize table name for avoiding SQL Injection Attacks
# 
# Arguments:
# table_name - variable containing the table_name received
# 
# Returns the Converted table_name without special characters
# and spaces separated by underlines (_)

def sanitize_table_name(table_name):
    table_name = table_name.lower()
    table_name = table_name.replace(' ', '_')
    table_name = re.sub('[^\\w\\s]+', '', table_name)
    return table_name
