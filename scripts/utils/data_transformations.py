import re

# TODO Incorporate column_types into function to make more efficient.
def convert_scientific_notation(current_row):
    """ Identifies scientific notation values then converts to float.

    :param current_row: The row that is to be converted.
    The current_row must be passed in as a Python Dictionary.
    """
    converted_row = {}

    for key, value in current_row.items():

        value = str(value)
        
        # Regex reads: one digit > period > zero or more digits > "E" or "e" > "+" or "-" > one or more digits.
        if re.search(r'^\d{1}[.]\d*[Ee][+-]\d+$', value):
            converted_row[key] = float(value)
            
        else:
            converted_row[key] = value

    return converted_row


# TODO Output the location / values of the malformed rows and fields to a log file.
def merge_split_rows(column_names, broken_row, row_generator):

    """ Will merge rows that have been broken into multiple parts thanks to a newline in one of the fields.

    :param column names: A List of column names to be returned in fixed row.
    :current_row: Current iteration of Generator object representing broken row.
    """
    row = row_generator
    
    # Start with a fresh row.
    pending_row = {}

    # Assign columns to be added to fresh row.
    pending_columns = column_names.copy()

    # Add all of the columns and values already present in broken row to pending row.
    for key, value in broken_row.items():
        pending_column = pending_columns.pop(0)
        pending_row[pending_column] = value

    # The last column present in broken row is the row with the newline.
    column_with_newline = pending_column
    print(f"Merging fields split by newline. Column with newline: {column_with_newline}\n{pending_row[column_with_newline]}")
    print(f"Current row data where the newline is currently being handled:\n {broken_row}")
        
    next_row = next(row)

    # An empty Dictionary is passed if two back to back newlines occur...
    # this statement replaces the empty dict with appropriate key/value.
    if len(next_row.keys()) == 0:
        next_row = {column_names[0] : "\n"}
   
    # Creates a List of all the values in the next Dictionary.
    next_row_values = list(next_row.values())
    
    # The first value in the next row was the field split in half by the newline.
    next_line = next_row_values.pop(0)

    if next_line == "\n":
        # if the next value is just a newline, concatenate the newline into the broken/split field
        pending_row[column_with_newline] += next_line
    else:
        # Because the newline is omitted from the string, add the newline back in before concatenating with field.
        pending_row[column_with_newline] += ("\n" + next_line)

    # For the remaining values, add the next pending column and current value to pending row.
    for value in next_row_values:
        pending_column = pending_columns.pop(0)
        pending_row[pending_column] = value
    
    # If all columns have not been added to pending row,
    # execute merge function again with pending row as broken row argument.
    if len(pending_columns) != 0:
        return merge_split_rows(column_names, pending_row, row_generator)
    else:
        # Removes trailing double quote from broken field.
        pending_row[column_with_newline] = pending_row[column_with_newline][0:-1]

        print(f"Fixed row:\n {pending_row}")

        return pending_row