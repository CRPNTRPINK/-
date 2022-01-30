def find_last_for_new(sheet, w):
    if w == 'column':
        return sheet.range('A1').end('right').column + 1
    elif w == 'row':
        return sheet.range('A1').end('right').row + 1


def find_last(sheet, w, expand='right'):
    if w == 'column':
        return sheet.range('A1').end(expand).column
    elif w == 'row':
        return sheet.range('A1').end(expand).row


def color(sheet, column_row):
    end = sheet.range(column_row).end('down').row
    start = int(column_row[1])
    column = column_row[0]
    for i in range(start, end + 1):
        row = sheet.range(f'{column}{i}')
        if row.value < 5:
            row.color = (0, 255, 0)  # зеленый
        elif 5 <= row.value <= 10:
            row.color = (255, 255, 0)  # желтый
        else:
            row.color = (255, 0, 0)  # красный

# def validate(sheet, column_row)
