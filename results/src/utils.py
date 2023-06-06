def pretty_size(num_bytes):
    units = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    index = 0

    while num_bytes >= 1024 and index < len(units) - 1:
        num_bytes /= 1024
        index += 1

    return f"{num_bytes:.2f} {units[index]}"
