def wait_time(e):
    wait_l = [int(word) for word in str(e).split() if word.isdigit()]
    wait = ''
    for digit in wait_l:
        wait += str(digit)
    return int(wait)