from datetime import datetime

def print_time(phase="unspecified", table="unspecified"):
    """
    Prints time to stdout / console in custom format.
    """

    now = datetime.now()
    now = f"{now.hour}:{now.minute}:{now.second}"
    init_start = f"Table: {table} has begun initializing. Time : {now}"
    init_complete = f"Table: {table} has initialized. Time : {now}"
    append_start = f"Table: {table} is being appended. Time : {now}"
    append_complete = f"Table: {table} successfully appended. Time : {now}"

    if phase == "unspecified":
        print(now)
    elif phase == "init_start":
        print(init_start)
    elif phase == "init_complete":
        print(init_complete)
    elif phase == "append_start":
        print(append_start)
    elif phase == "append_complete":
        print(append_complete)
    else:
        print("Qué?" + " " + now)