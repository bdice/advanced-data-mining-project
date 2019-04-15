import pandas as pd
import numpy as np

mapping = '/hadoop-fuse/var/eecs598w19/users/group_3/workspace/6ae284602fc56465315a18f804b30bcb/airport_codes.csv'
ID_output = './first-order-airports-PR.csv'
code_output = './first-order-airports-PR-codes.csv'

df = pd.read_csv(mapping, dtype='str')
dict = df.to_dict(orient='list')
print(dict)
ID = []
value = []

with open(ID_output) as f:
    for line in f:
        fields = line.strip().split(',')
        ID.append(fields[0])
        value.append(fields[1])

ID = np.array(ID)
value = np.array(value)

#for i in
