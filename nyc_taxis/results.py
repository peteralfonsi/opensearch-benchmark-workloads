import openpyxl

workbook = openpyxl.load_workbook('template.xlsx')
worksheet = workbook['Sheet1']
data_to_fill = {
    (1, 1): 'All results in milliseconds',
    (1, 2): 'Jan 1 to Jan 1',
    (1, 3): 'Jan 1 to Jan 2',
    (1, 4): 'Jan 1 to Jan 3',
    (1, 5): 'Jan 1 to Jan 4',
    (1, 6): 'Jan 1 to Jan 5',
    (1, 7): 'Jan 1 to Jan 6',
    (1, 8): 'Jan 1 to Jan 7',
    (2, 1): 'Average Response Time',
    (4, 1): 'PoC Disk Only',
    (5, 1): 'PoC Disk + Heap (30MB)',
    (6, 1): 'PoC Heap Only',
    (7, 1): 'OpenSearch Heap',
    (9, 1): 'Median Response Time',
    (11, 1): 'PoC Disk Only',
    (12, 1): 'PoC Disk + Heap (30MB)',
    (13, 1): 'PoC Heap Only',
    (14, 1): 'OpenSearch Heap',
    (16, 1): 'p99 Latency',
    (18, 1): 'PoC Disk Only',
    (19, 1): 'PoC Disk + Heap (30MB)',
    (20, 1): 'PoC Heap Only',
    (21, 1): 'OpenSearch Heap',
    (23, 1): 'p95 Latency',
    (25, 1): 'PoC Disk Only',
    (26, 1): 'PoC Disk + Heap (30MB)',
    (27, 1): 'PoC Heap Only',
    (28, 1): 'OpenSearch Heap',
    (30, 1): 'p90 Latency',
    (32, 1): 'PoC Disk Only',
    (33, 1): 'PoC Disk + Heap (30MB)',
    (34, 1): 'PoC Heap Only',
    (35, 1): 'OpenSearch Heap',
    (37, 1): 'Minimum',
    (39, 1): 'PoC Disk Only',
    (40, 1): 'PoC Disk + Heap (30MB)',
    (41, 1): 'PoC Heap Only',
    (42, 1): 'OpenSearch Heap',   
    (44, 1): 'Minimum',
    (46, 1): 'PoC Disk Only',
    (47, 1): 'PoC Disk + Heap (30MB)',
    (48, 1): 'PoC Heap Only',
    (49, 1): 'OpenSearch Heap'
}

for cell_coordinates, value in data_to_fill.items():
    row, col = cell_coordinates
    cell = worksheet.cell(row=row, column=col)
    cell.value = value

workbook.save('template.xlsx')