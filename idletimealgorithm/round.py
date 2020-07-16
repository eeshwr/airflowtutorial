def roundoff(number):
    strnum = str(number)
    dot = strnum.find('.')
    back = strnum[dot:]
    backint  = float(back)
    frontnum = int(strnum[:dot])
    if backint >= 0.5:
        print(frontnum+1)
    else:
        print(frontnum)
