# region Initial_Param
prs = argparse.ArgumentParser(description='This Program Write For Undecided Transaction 780.ir co')

prs.add_argument('cfg_file', action='store', help='configFile.ini path', default="cfgDir/configFile.ini")
prs.add_argument('log_dir', action='store', help='log directory path', default="logDir/")
prs.add_argument('arch_dir', action='store', help='archive create file directory path', default="temp/")
prs.add_argument('send_ftp', action='store', help='send ftp input [TRUE] or [FALSE] ?', default="FALSE")
prs.add_argument('--debug', action='store_true', help='print debug messages to stderr')

args = prs.parse_args()
__level__ = logging.INFO
__LOGDIR__ = os.path.abspath(args.log_dir)

try:
    __confiFileName__ = os.path.abspath(args.cfg_file)
    __SendFTP__ = str(args.send_ftp)
    __ARC__ = str(args.arch_dir)
except:
    print("Noting variable set")

logMain = Logger(filename="main_init", level=__level__,
                 dirname="File-" + os.path.basename(__file__), rootdir=__LOGDIR__)

# endregion Initial_Param