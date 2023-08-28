import pandas

date_from = pandas.Timestamp(day=1, month=9, year=2023)
date_to = pandas.Timestamp(day=5, month=9, year=2023)

a = (date_to - date_from).days
print(a)
