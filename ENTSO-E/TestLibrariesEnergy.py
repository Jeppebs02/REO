from datetime import datetime

# Get the current date
current_date = datetime.now()

# Get the day of the year
day_of_year = current_date.timetuple().tm_yday

print(day_of_year)


current_date_timestamp = current_date.strftime("%Y-%m-%d")

# Remove unwanted characters from the formatted date
current_date_timestamp = current_date_timestamp.replace("-", "")

print(current_date_timestamp)



current_date2=datetime.date.today()
start_date = datetime.date(current_date2.year, 1, 1)
print(str(start_date))