import socket
import sys
from ftplib import FTP_TLS

ftps = FTP_TLS(timeout=15)
ftps.set_debuglevel(2)
ftps.connect('172.30.23.26', 21)
ftps.login('mahdi.gh@780.ir', 'Zz12345678')
ftps.set_pasv(True)
ftps.prot_p()

ftps.cwd('/EN')
ftps.retrlines('NLST')

# the name of file you want to download from the FTP server
filename = "EN_581672031-14000107-1.txt"
with open(filename, "wb") as file:
    # use FTP's RETR command to download the file
    ftps.retrbinary(f"RETR {filename}", file.write)

ftps.quit()
ftps.close()
