from datetime import datetime

def print_time(phase="unspecified", table="unspecified"):
    """
    Prints time to stdout / console in custom format.

    :param phase (str): accepts "csv_start" and "csv_complete"
    :param table (str): name of table
    """

    now = datetime.now()
    now = f"{now.hour}:{now.minute}:{now.second}"
    csv_start = f"CSV for table: {table} is being created. Time : {now}"
    csv_complete = f"CSV for table: {table} has been created!. Time : {now}"

    if phase == "unspecified":
        print(now)
    elif phase == "csv_start":
        print(csv_start)
    elif phase == "csv_complete":
        print(csv_complete)
    else:
        print("Qué?" + " " + now)