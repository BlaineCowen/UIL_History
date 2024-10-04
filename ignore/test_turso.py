import os
import pandas as pd
import numpy as np
import libsql_experimental as libsql

# get env
auth_token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3MTcwNTI5ODgsImlkIjoiY2VhZjExZWUtNTllZS00NWY5LWIzOTYtYmQyMGU5NGI1MzMzIn0.TfJIw8aSzIvTETIT8znhkDCgf5M60niJUWxHWqws3TXlqCvd_q9pMRq_I0Wxi7EeqtzYVp5rRLKscKymeN4uDA"
url = "libsql://uil-blainecowen.turso.io"

# get data
conn = libsql.connect("uil.db", sync_url=url, auth_token=auth_token)
conn.sync()

df = pd.read_sql("SELECT * FROM results", conn)

print(df.head())
