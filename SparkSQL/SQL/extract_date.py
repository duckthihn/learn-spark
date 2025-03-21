data = [("11/12/2025",), ("27/02/2014",), ("2023.01.09",), ("28-12-2005",)]

def extract_date(data_str):
    for delimiter in ['/','.','-']:
        if delimiter in data_str:
            day, month, year = data_str.split(delimiter)
            if len(year) == 4:
                return day, month, year
            else:
                return year, month, day

