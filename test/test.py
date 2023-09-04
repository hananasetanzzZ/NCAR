from datetime import datetime, timedelta

start_date = datetime.strptime('0828', '%m%d')
end_date = datetime.strptime('0902', '%m%d')

date_list = []
current_date = start_date

while current_date <= end_date:
    date_str = current_date.strftime('%m%d')
    date_list.append(date_str)
    current_date += timedelta(days=1)

print(date_list)
