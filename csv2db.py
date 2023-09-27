# D:/code/wsl/playground/csv2db

import os
import re
import csv
from db_config.db_connect import dbConnect
from string import Template
from itertools import islice, filterfalse


def get_tab_length(row_data):
    """
    Returns the length of the tab required to separate the columns in the row data 
    """
    max_column_width = 0
    for column in row_data:
        max_column_width = max(max_column_width, len(column))

    return max_column_width + 1

def write_row_with_tab_separator(row_data, output_file):
    """
    Writes the row data to the output file with a tab separator.
    """
    tab_length = get_tab_length(row_data)
    tab_separator = ' ' * tab_length
    output_file.write(tab_separator.join(row_data) + '\n')

def get_txtFileName(phoneNumb,data):
    """
    Create File name for txt file.
    """

    # get the first date from data
    first_date = data[1][0][:2]

    # get the last date from from data
    last_date = data[-1][0][:2]

    # get the month and year from data
    monthYear = data[1][0][3:]
    
    # file name "bill_phonenumb_firstdate-lastdate-month-year"
    return "bill_" + phoneNumb + "_" + str(first_date) + "-" + str(last_date) + "-" + monthYear.replace(" ", "-") + ".txt"


def createTable(data, dbCon):
    """
    Create a SQL TABLE 
    """


    # Get the column names from the data
    column_names = data[0]


    sql = 'CREATE TABLE IF NOT EXISTS phone_bill ('
    for column_name in column_names[:-1]:
        sql += f'{column_name.lower()} VARCHAR(255) NOT NULL, '
    sql += f'{column_names[-1].lower()} VARCHAR(255) NOT NULL);'

    dbCon.execute(sql)
    
def LoadCSV():

    # Database Assign
    db = dbConnect(5, True)
    
    
    # Assign variable location
    sourcePath = os.getcwd()+"/"
    outputLPath = os.getcwd()+"/result"

    # File variable
    searchString = "MyUsage"
    fileType = ".csv"

    # File Name Uniq's string
    uniqString = "0435363247"

    # pattern
    date_pattern = r'(0?[1-9]|[12][0-9]|3[01]) [A-Za-z]+ [0-9]+)'

    # list all the files in source location
    fileList = [f for f in os.listdir(sourcePath) if f.endswith(fileType)]

    # loop file in the directory
    for file in fileList:
        if uniqString in file:

            # Initalize variables
            tables = []
            
            with open(os.path.join(sourcePath, file), 'r') as f:
                csv_reader = csv.reader(f, )
                
                found_header = False
                header = []
                date_value = []
                table = list()


                for line in csv_reader:

                    # Clean unwanted line
                    if line[0].startswith(("Phone Number", "Summary", "Cost", "$")):
                        continue
                    
                    # Get header
                    if not found_header:
                        for item in line:
                            if item in ("Type", "Time", "Number", "Duration", "Quantity", "Cost$"):
                                header.append(item)
                                found_header = True
                    
                    # Skip if row is Header 
                    match = [item for item in line if item in ("Type", "Time", "Number", "Duration", "Quantity", "Cost$")]
                    if match:
                        continue

                    # Pattern regrex for get date on top header
                    date_pattern = re.compile(r"[0-9]+\s[A-Za-z]+\s+[0-9]+", re.IGNORECASE)
                    date_ = date_pattern.match(line[0])
                    if date_:
                        date_value = date_.group()
                        continue        
                    
                    # add date value colum into data
                    line.insert(0, date_value)

                    # create new list of data
                    table.append(line)

                # Add "Date" into first column of header
                header.insert(0, "Date")

                file_name = get_txtFileName(uniqString, table[::-1])

                # Add Header to data
                table.insert(0, header)

                # Create a table in MySQL Database
                createTable(header, db)



                # Create a text file for writing
                with open(file_name, 'w') as txt_file:
                    # Iterate over the data table and write each row to the text file

                    list_value = list()
                    for row in islice(table, 0, None, 1):

                        write_row_with_tab_separator(row, txt_file)

                        head_string = "Date"
                        # skip saving into database if row is header
                        if head_string in row:
                            continue
                        
                        list_val = row[0], row[1], row[2], row[3], row[4], row[5]

                        # Prepare the SQL INSERT statement
                        Template_SQL = ("INSERT INTO phone_bill (date, type, time, number, duration, quantity) VALUES $list_values;\n")
                        
                    
                        strSQL = Template(Template_SQL).substitute(
                            list_values = list_val,
                            date = "'" + row[0] + "'",
                            type = "'" + row[1] + "'",
                            time = "'" + row[2] + "'",
                            number = "'" + row[3] + "'"
                        )

                        # Insert the row into the MySQL Database
                        db.execute(strSQL)
                    
                    db.close()
            
if __name__ == '__main__':
    LoadCSV()
    