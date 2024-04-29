name = 'util'

import datetime as dt

def read_apikey(filePath):
    """

    Parameters
    ----------
    filePath : string
        Path to text file containing SportsDataIO API Key.

    Returns
    -------
    apikey : string
        Contents of the file at filePath.

    """
    apikey = open("{}".format(filePath), 'r').read()
    return apikey


def convert_object2date(objectIn, formatIn):
    """

    Parameters
    ----------
    objectIn : string
        Imported datetime value stored as string (object).
    
    formatIn : string
        Format of the imported datetime value.

    Returns
    -------
    dateOut : date
        Converted datetime to date.

    """
    dateTime = dt.datetime.strptime(objectIn, formatIn)
    dateOut  = dt.date(dateTime.year, dateTime.month, dateTime.day)
    return dateOut


def espn_pga_schedule_start_date(strIn):
    strOut = strIn.split(" - ")[0].split(" ")[0].strip()
    return strOut


def clean_espn_pga_schedule_date(strIn, yearIn, returnType):
    lstStr = strIn.split(" - ")
    if len(lstStr) == 1:
        startDate = dt.date(
            int(yearIn),
            dt.datetime.strptime(lstStr[0], "%b %d").month,
            dt.datetime.strptime(lstStr[0], "%b %d").day
            )
        endDate = startDate
    else:
        if len(lstStr[0].split(" ")) == len(lstStr[1].split(" ")):
            strTmp = yearIn + " " + lstStr[0]
            startDate = dt.date(
                int(yearIn),
                dt.datetime.strptime(strTmp, "%Y %b %d").month,
                dt.datetime.strptime(strTmp, "%Y %b %d").day
                )
            endDate   = dt.date(
                int(yearIn),
                dt.datetime.strptime(lstStr[1], "%b %d").month,
                dt.datetime.strptime(lstStr[1], "%b %d").day
                )
        else:
            startDate = dt.date(
                int(yearIn),
                dt.datetime.strptime(lstStr[0], "%b %d").month,
                dt.datetime.strptime(lstStr[0], "%b %d").day
                )
            endDate   = dt.date(
                int(yearIn),
                dt.datetime.strptime(lstStr[0], "%b %d").month,
                int(lstStr[1])
                )
    if returnType.lower() == "start":
        dateOut = startDate
    if returnType.lower() == "end":
        dateOut = endDate
    return dateOut

