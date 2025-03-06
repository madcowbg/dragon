def format_size(size: int) -> str:
    if size < 2 ** 10:
        return f"{size}"
    elif size < 2 ** 20:
        return f"{size / 2 ** 10:.1f}KB"
    elif size < 2 ** 30:
        return f"{size / 2 ** 20:.1f}MB"
    elif size < 2 ** 40:
        return f"{size / 2 ** 30:.1f}GB"
    elif size < 2 ** 50:
        return f"{size / 2 ** 40:.1f}TB"
    else:
        return f"{size / 2 ** 50:.1f}PB"
