from ftplib import FTP_TLS
ftps = FTP_TLS('172.30.23.26')
# login anonymously before securing control channel
ftps.login('mahdi.gh@780.ir', 'Zz12345678')
# switch to secure data connection.. IMPORTANT!
# Otherwise, only the user and password is encrypted and not all the file data.
ftps.prot_p()
ftps.retrlines('LIST')

filename = '14000615.Batch'
print ('Opening local file ' + filename)
myfile = open(filename, 'wb')

ftps.retrbinary('RETR %s' % filename, myfile.write)

ftps.close()