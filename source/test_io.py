import pandas as pd
import os
from io import BytesIO
from pathlib import Path

P_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
OUTPUT_DIR = os.path.join(P_DIR, 'outputs')

# create dataframe
df_marks = pd.DataFrame({'name': ['Somu', 'Kiku', 'Amol', 'Lini'],
     'physics': [68, 74, 77, 78],
     'chemistry': [84, 56, 73, 69],
     'algebra': [78, 88, 82, 87]})

output = BytesIO()
writer = pd.ExcelWriter(output, engine='xlsxwriter')
for x in range(2):
    df_marks.to_excel(excel_writer=writer, sheet_name="sheet_" + str(x))

writer.save()

Path(os.path.join(OUTPUT_DIR, 'test_attachment.xlsx')).write_bytes(output.getbuffer())
