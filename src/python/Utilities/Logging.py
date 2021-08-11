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
        print(f"Failed to display time of {seconds} [s]\n{str(error)}")


def displayNumber(n: int) -> str:
    """
    The function to display a number for logging
    :param n: number
    :return: number in K, M or B
    """
    try:
        if not str(n).isdigit():
            return str(n)

        k, _ = divmod(n, 1000)
        m, k = divmod(k, 1000)
        b, m = divmod(m, 1000)

        return f"{b}B" if b else f"{m}M" if m else f"{k}K" if k else str(n)

    except Exception as error:
        print(f"Failed to display number {n}\n{str(error)}")
