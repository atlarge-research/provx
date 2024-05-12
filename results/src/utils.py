def format_seconds(x):
    hours = int(x // 3600)
    minutes = int((x % 3600) // 60)
    seconds = int(x % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_filesize(x):
    # Define size units
    size_units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    unit_index = 0

    # Convert bytes to higher units as needed
    while x >= 1024 and unit_index < len(size_units) - 1:
        x /= 1024
        unit_index += 1

    return (x, size_units[unit_index])
