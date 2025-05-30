def generate_batch(arr: list, batch_size: int) -> list:
    return [
        arr[i:i + batch_size]
        for i in range(0, len(arr), batch_size)
    ]
