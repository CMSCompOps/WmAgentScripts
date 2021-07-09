def displayTime(seconds: int) -> str:
    """
    The function to display time for logging
    :param seconds: time in seconds
    :return: time in days, hours, minutes and seconds
    """
    try:
        if not seconds:
            return seconds

        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        return f"{days} [d] {hours} [h] {minutes} [m] {seconds} [s]"

    except Exception as error:
        print(f"Failed to display time of {seconds} [s]")
        print(str(error))
