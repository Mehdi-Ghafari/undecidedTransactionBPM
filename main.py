import os


def mergeFile():
    pathEN2 = os.path.abspath('780\EN\EN_581672031-14000613-1.txt')
    pathENT = os.path.abspath('780\EN TARAKONESH\EN_581672031-14000613-2.txt')
    pathOUT = os.path.abspath('780\OUT\outSAMPLE.txt')

    with open(pathEN2) as file1:
        txt1 = file1.read().splitlines()
        print(txt1)

    with open(pathENT) as file2:
        txt2 = file2.read().splitlines()
        print(txt2)

    with open(pathOUT, 'w') as new_file:
        new = ''
        for line_a in txt1:
            for line_b in txt2:
                if line_a.split("|")[3] == line_b.split("|")[4] \
                        and line_a.split("|")[4] == line_b.split("|")[5] \
                        and line_a.split("|")[5] == line_b.split("|")[6]:
                    new_file.write(
                        line_b.split("|")[0] + "|" +
                        line_a.split("|")[0] + "|" + line_a.split("|")[1] + "|" + line_a.split("|")[2] + "|" +
                        line_a.split("|")[3] + "|" +
                        line_a.split("|")[4] + "|" + line_a.split("|")[5] + "|" + line_a.split("|")[6] + "|" +
                        line_a.split("|")[7] + "|" +
                        line_a.split("|")[8] + "|" + line_a.split("|")[9] + "|" + line_a.split("|")[10]
                        + '\n')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    mergeFile()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
