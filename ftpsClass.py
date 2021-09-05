import ftplib
import ssl
import socket
import logging

_SSLSocket = ssl.SSLSocket
__level__ = logging.INFO


class tyFTP(ftplib.FTP_TLS):
    def __init__(self,
                 host='',
                 user='',
                 passwd='',
                 acct='',
                 keyfile=None,
                 certfile=None,
                 timeout=60):

        ftplib.FTP_TLS.__init__(self,
                                host=host,
                                user=user,
                                passwd=passwd,
                                acct=acct,
                                keyfile=keyfile,
                                certfile=certfile,
                                timeout=timeout)

    # def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
    #     self.voidcmd('TYPE I')
    #     with self.transfercmd(cmd, rest) as conn:
    #         while 1:
    #             buf = fp.read(blocksize)
    #             if not buf:
    #                 break
    #             conn.sendall(buf)
    #             if callback:
    #                 callback(buf)
    #         # shutdown ssl layer
    #         if _SSLSocket is not None and isinstance(conn, _SSLSocket):
    #             try:
    #                 # conn.unwrap()
    #                 print("pass")
    #             except Exception as e:
    #                 print(e)
    #     try:
    #         return self.voidresp()
    #     except Exception as e:
    #         print(e)

    def storbinary(self, cmd, fp, blocksize=8192, callback=None, rest=None):
        """Store a file in binary mode.  A new port is created for you.

        Args:
          cmd: A STOR command.
          fp: A file-like object with a read(num_bytes) method.
          blocksize: The maximum data size to read from fp and send over
                     the connection at once.  [default: 8192]
          callback: An optional single parameter callable that is called on
                    each block of data after it is sent.  [default: None]
          rest: Passed to transfercmd().  [default: None]

        Returns:
          The response code.
        """
        self.voidcmd('TYPE I')
        conn = self.transfercmd(cmd, rest)
        try:
            while 1:
                buf = fp.read(blocksize)
                if not buf:
                    break
                conn.sendall(buf)
                if callback:
                    callback(buf)
            if isinstance(conn, ssl.SSLSocket):
                pass
        #         conn.unwrap()
        finally:
            conn.close()
        return self.voidresp()

    def connect(self, host='', port=0, timeout=-999, **kwargs):
        """
        Connect to host.  Arguments are:
            - host: hostname to connect to (string, default previous host)
            - port: port to connect to (integer, default previous port)
            :param timeout: -999
            :param host: ''
            :param port: 0
        """
        if host != '':
            self.host = host
        if port > 0:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        try:
            self.sock = socket.create_connection((self.host, self.port), self.timeout)
            self.af = self.sock.family
            # add this line!!!
            self.sock = ssl.wrap_socket(self.sock,
                                        self.keyfile,
                                        self.certfile,
                                        # cert_reqs=ssl.CERT_NONE,
                                        # ssl_version=ssl.PROTOCOL_TLSv1_2,
                                        # certfile="ssl/win2012r2.crt", keyfile="ssl/win2012r2.key",
                                        ssl_version=ssl.PROTOCOL_TLSv1)  # this is the fix
            # add end
            self.file = self.sock.makefile('r')
            self.welcome = self.getresp()
        except Exception as e:
            print(e)
        return self.welcome


def connect_ftp(ftpServer, ftpPort, ftpUser, ftpPass):
    ftplib.FTP_PORT = ftpPort
    ftp = tyFTP(timeout=10)

    ftp.set_debuglevel(False)
    # ftp.set_debuglevel(2)
    # print(ftp.debugging)
    # ftp.set_pasv(False)
    # ftp.connect(server, 990)
    # ftp.auth()
    try:
        ftp.connect(ftpServer, int(ftpPort))
        ftp.auth()
    except Exception as e:
        # logConnectFtp.error("Error Connect Ftp: " + e)
        print(e)
    ftp.login(ftpUser, ftpPass)
    ftp.prot_p()
    # ftp.nlst()

    # files = []
    #
    # try:
    #     files = ftp.nlst("/")
    # except ftplib.error_perm as resp:
    #     if str(resp) == "550 No files found":
    #         print("No files in this directory")
    #     else:
    #         raise
    # for f in files:
    #     print(f)

    return ftp
