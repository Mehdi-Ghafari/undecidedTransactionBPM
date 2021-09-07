#!/usr/bin/env python
# -*- coding: windows-1256 -*-
import argparse
import re
import traceback
import cx_Oracle
import logging
import sys
from configparser import ConfigParser
import dbHelper
from Logger import Logger
import os
from ftpsClass import connect_ftp
import datetime

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

# region Function hunterCursor
def chunks(cur):  # 65536
    global log, d
    while True:
        # log.info('Chunk size %s' %  cur.arraysize, extra=d)
        rows = cur.fetchmany()

        if not rows: break;
        yield rows


# endregion Function hunterCursor

# region Function LoadFromView
def loadFromView():
    logloadFromView = Logger(filename="__init__", level=__level__,
                             dirname="File-" + os.path.basename(
                                 __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)

    sqlViewPNA = """
                    SELECT
                        LOWER(DIAG) DIAG,
                        PSP_BIN,
                        TERMINAL,
                        AUT_DATE,
                        AMOUNT,
                        TRACE,
                        BIN_CARD,
                        RPAD(PAN, 19, ' ') PAN,
                        PROCCESS_CODE,
                        DEVICE_TYPE,
                        SPLIT_COLMN,
                        STATUS,
                        PKID,
                        LOWER(FILENAME) FILENAME,
                        FILEDATE,
                        INSERTDATE,
                        RRN_DWH,
                        FILEMILADIDATE
                    FROM
                        TEHRAN_INTERNET.ACQ_INQ_780_WITH_RRN
                    WHERE FILEDATE > :TEST

            """

    try:
        # cursorPNA.execArgs(sqlViewPNA, V_MAX_FILEDATE, __LOGDIR__ + '/ORA')
        # cursorPNA.execute(sqlViewPNA, {"V_PKID": V_MAX})
        # cursorPNA.arraysize = 20
        cursorPNA.execute(sqlViewPNA, test=V_MAX_FILEDATE)
        recordsPNA = cursorPNA.fetchall()

        try:
            # No commit as you don-t need to commit DDL.
            global Vn_DIAG
            Vn_DIAG = [record[0] for record in recordsPNA] or ''

            global Vn_PSP_BIN
            Vn_PSP_BIN = [record[1] for record in recordsPNA] or ''

            global Vn_TERMINAL
            Vn_TERMINAL = [record[2] for record in recordsPNA] or ''

            global Vn_AUT_DATE
            Vn_AUT_DATE = [record[3] for record in recordsPNA] or ''

            global Vn_AMOUNT
            Vn_AMOUNT = [record[4] for record in recordsPNA] or ''

            global Vn_TRACE
            Vn_TRACE = [record[5] for record in recordsPNA] or ''

            global Vn_BIN_CARD
            Vn_BIN_CARD = [record[6] for record in recordsPNA] or ''

            global Vn_PAN
            Vn_PAN = [record[7] for record in recordsPNA] or ''

            global Vn_PROCCESS_CODE
            Vn_PROCCESS_CODE = [record[8] for record in recordsPNA] or ''

            global Vn_DEVICE_TYPE
            Vn_DEVICE_TYPE = [record[9] for record in recordsPNA] or ''

            global Vn_SPLIT_COLMN
            Vn_SPLIT_COLMN = [record[10] for record in recordsPNA] or ''

            global Vn_STATUS
            Vn_STATUS = [record[11] for record in recordsPNA] or ''

            global Vn_PKID
            Vn_PKID = [record[12] for record in recordsPNA] or ''

            global Vn_FILENAME
            Vn_FILENAME = [record[13] for record in recordsPNA] or ''

            global Vn_FILEDATE
            Vn_FILEDATE = [record[14] for record in recordsPNA] or ''

            global Vn_INSERTDATE
            Vn_INSERTDATE = [record[15] for record in recordsPNA] or ''

            global Vn_RRN_DWH
            Vn_RRN_DWH = [record[16] for record in recordsPNA] or ''

            global Vn_FILEMILADIDATE
            Vn_FILEMILADIDATE = [record[17] for record in recordsPNA] or ''

            NewMaxFILEDATE(Vn_FILEDATE[0])

            global listforIterable
            listforIterable = zip(
                Vn_DIAG,
                Vn_PSP_BIN, Vn_TERMINAL, Vn_AUT_DATE, Vn_AMOUNT, Vn_TRACE,
                Vn_BIN_CARD, Vn_PAN, Vn_PROCCESS_CODE, Vn_DEVICE_TYPE, Vn_SPLIT_COLMN, Vn_STATUS, Vn_PKID,
                Vn_FILENAME, Vn_FILEDATE, Vn_INSERTDATE, Vn_RRN_DWH, Vn_FILEMILADIDATE)
        # Ensure that we always disconnect from the database to avoid
        # ORA-00018: Maximum number of sessions exceeded.
        except IndexError as ex:
            logloadFromView.error("ERROR MASSAGE: " + str(ex))

        except (IOError, OSError):
            logloadFromView.error("OS ERROR: " + str(OSError))
            logloadFromView.error("OR OS IOError: " + str(IOError))

        except cx_Oracle.DatabaseError as ex:
            logloadFromView.warning("GENERAL ERROR DATABASE IN FUNCTION loadFromView")
            logloadFromView.error("ERROR MASSAGE: " + str(ex))

        except:
            logloadFromView.error("UNEXPECTED ERROR FUNCTION loadFromView:" + str(sys.exc_info()[0]))

    except Exception as e:
        logloadFromView.error("DB ERROR: " + str(e))


"""
    global Vn_DIAG

    for i, recordsPNA in enumerate(chunks(cursorPNA)):

        try:
            # No commit as you don-t need to commit DDL.
            print(i)
            Vn_DIAG[i] = [record[0] for record in recordsPNA] or ''
            print(Vn_DIAG[i])

            global Vn_PSP_BIN
            Vn_PSP_BIN = [record[1] for record in recordsPNA] or ''

            global Vn_TERMINAL
            Vn_TERMINAL = [record[2] for record in recordsPNA] or ''

            global Vn_AUT_DATE
            Vn_AUT_DATE = [record[3] for record in recordsPNA] or ''

            global Vn_AMOUNT
            Vn_AMOUNT = [record[4] for record in recordsPNA] or ''

            global Vn_TRACE
            Vn_TRACE = [record[5] for record in recordsPNA] or ''

            global Vn_BIN_CARD
            Vn_BIN_CARD = [record[6] for record in recordsPNA] or ''

            global Vn_PAN
            Vn_PAN = [record[7] for record in recordsPNA] or ''

            global Vn_PROCCESS_CODE
            Vn_PROCCESS_CODE = [record[8] for record in recordsPNA] or ''

            global Vn_DEVICE_TYPE
            Vn_DEVICE_TYPE = [record[9] for record in recordsPNA] or ''

            global Vn_SPLIT_COLMN
            Vn_SPLIT_COLMN = [record[10] for record in recordsPNA] or ''

            global Vn_STATUS
            Vn_STATUS = [record[11] for record in recordsPNA] or ''

            global Vn_PKID
            Vn_PKID = [record[12] for record in recordsPNA] or ''

            global Vn_FILENAME
            Vn_FILENAME = [record[13] for record in recordsPNA] or ''

            global Vn_FILEDATE
            Vn_FILEDATE = [record[14] for record in recordsPNA] or ''

            global Vn_INSERTDATE
            Vn_INSERTDATE = [record[15] for record in recordsPNA] or ''

            global Vn_RRN_DWH
            Vn_RRN_DWH = [record[16] for record in recordsPNA] or ''

            global Vn_FILEMILADIDATE
            Vn_FILEMILADIDATE = [record[17] for record in recordsPNA] or ''

            NewMaxFILEDATE(Vn_FILEDATE[0])

            # global listforIterable
            # listforIterable = zip(
            #     Vn_DIAG,
            #     Vn_PSP_BIN, Vn_TERMINAL, Vn_AUT_DATE, Vn_AMOUNT, Vn_TRACE,
            #     Vn_BIN_CARD, Vn_PAN, Vn_PROCCESS_CODE, Vn_DEVICE_TYPE, Vn_SPLIT_COLMN, Vn_STATUS, Vn_PKID,
            #     Vn_FILENAME, Vn_FILEDATE, Vn_INSERTDATE, Vn_RRN_DWH, Vn_FILEMILADIDATE)

            # for i in listforIterable:
            #     print(i)

        # Ensure that we always disconnect from the database to avoid
        # ORA-00018: Maximum number of sessions exceeded.

        except (IOError, OSError):
            print("OS ERROR")

        except cx_Oracle.DatabaseError as ex:
            LOG_loadFromView.warning("GENERAL ERROR DATABASE IN FUNCTION loadFromView")
            LOG_loadFromView.error("ERROR MASSAGE: " + str(ex))
        except:
            LOG_loadFromView.error("UNEXPECTED ERROR FUNCTION loadFromView:" + str(sys.exc_info()[0]))
"""


# endregion Function LoadFromView

# region Function writeNewMaxFILEDATE
def NewMaxFILEDATE(V_MAX):
    LOG_loadFromView = Logger(filename="__init__", level=__level__,
                              dirname="File-" + os.path.basename(
                                  __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)

    config = ConfigParser()
    config.read(__confiFileName__)
    config['GENERAL']['V_MAX_FILEDATE'] = str(V_MAX)

    try:
        with open(__confiFileName__, 'w', encoding='windows-1256') as configfile:  # save
            config.write(configfile)

    except IndexError as ex:
        LOG_loadFromView.error("ERROR MASSAGE: " + str(ex))

    except EnvironmentError as ex:  # parent of IOError, OSError *and* WindowsError where available
        LOG_loadFromView.warning("Error in create" + __confiFileName__ + " -> NewMaxID()")
        LOG_loadFromView.error("Error Massage: " + str(ex))
    except:
        LOG_loadFromView.error("Unexpected error:" + sys.exc_info()[0])


# endregion Function writeNewMaxFILEDATE

# region Function CALLFUNC_CHECK_TOPUP_STATUS
def CALLFUNC_CHECK_TOPUP_STATUS(v_msisdn=None, v_trace=None, v_desc=None, v_adddata=None, v_amount=0, v_rrn=None,
                                v_capdate=None):
    LOG_CALLFUNC_CHECK_TOPUP_STATUS = Logger(filename="__init__", level=__level__,
                                             dirname="File-" + os.path.basename(
                                                 __file__) + "-Func-" + sys._getframe().f_code.co_name,
                                             rootdir=__LOGDIR__)
    P_MSISDN = v_msisdn
    P_TRACE = v_trace
    P_DESC = v_desc
    P_ADDDATA = v_adddata
    P_AMOUNT = v_amount
    P_RESERVE1 = v_rrn
    P_RESERVE2 = v_capdate
    R_RESERVE1 = cursor780.var(str)
    R_RESERVE2 = cursor780.var(str)

    if __debugMODE__.upper() == 'TRUE':
        Important_single_log("Func_CALLFUNC_CHECK_TOPUP_STATUS ",
                             "all income variable name "
                             + "\n v_msisdn: " + str(P_MSISDN)
                             + "\n v_trace: " + str(P_TRACE)
                             + "\n v_desc: " + str(P_DESC)
                             + "\n v_adddata: " + str(P_ADDDATA)
                             + "\n v_amount: " + str(P_AMOUNT)
                             + "\n v_rrn: " + str(P_RESERVE1)
                             + "\n v_capdate: " + str(P_RESERVE2)
                             + "\n ------------------------"
                             )

    try:
        V_RET = cursor780.callfunc('USSD.CHECK_TOPUP_STATUS2', int, [P_MSISDN, P_TRACE, P_DESC, P_ADDDATA, P_AMOUNT,
                                                                     P_RESERVE1, P_RESERVE2, R_RESERVE1, R_RESERVE2])
        if __debugMODE__.upper() == 'TRUE':
            Important_single_log("Func_CALLFUNC_CHECK_TOPUP_STATUS ",
                                 "all return variable name "
                                 + "\n v_return: " + str(V_RET)
                                 + "\n v_reserve1: " + str(R_RESERVE1.getvalue())
                                 + "\n v_reserve2: " + str(R_RESERVE2.getvalue())
                                 + "\n ------------------------"
                                 )

        return V_RET, R_RESERVE1.getvalue(), R_RESERVE2.getvalue()

    except IndexError as ex:
        LOG_CALLFUNC_CHECK_TOPUP_STATUS.error("ERROR MASSAGE: " + str(ex))

    except cx_Oracle.DatabaseError as ex:
        LOG_CALLFUNC_CHECK_TOPUP_STATUS.warning("GENERAL ERROR DATABASE IN -> CALLFUNC_CHECK_TOPUP_STATUS()")
        LOG_CALLFUNC_CHECK_TOPUP_STATUS.error("ERROR MASSAGE: " + str(ex))
    except:
        LOG_CALLFUNC_CHECK_TOPUP_STATUS.error(
            "UNEXPECTED ERROR FUNCTION -> CALLFUNC_CHECK_TOPUP_STATUS:" + str(sys.exc_info()[0]))
        return -1


# endregion Function CALLFUNC_CHECK_TOPUP_STATUS

# region Function CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE
def CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE(
        v_diag=None, v_psp_bin=None, v_terminal=None, v_aut_date=None, v_amount=None, v_trace=None, v_bin_card=None,
        v_proccess_code=None, v_device_type=None, v_split_colmn=None, v_status=None, v_pkid=None,
        v_filename=None, v_filedate=None, v_insertdate=None, v_rrn_dwh=None, v_filemiladidate=None
):
    p_diag = v_diag
    p_psp_bin = v_psp_bin
    p_terminal = v_terminal
    p_aut_date = v_aut_date
    p_amount = v_amount
    p_trace = v_trace
    p_bin_card = v_bin_card
    p_proccess_code = v_proccess_code
    p_device_type = v_device_type
    p_split_colmn = v_split_colmn
    p_status = v_status
    p_pkid = v_pkid
    p_filename = v_filename
    p_filedate = v_filedate
    p_insertdate = v_insertdate
    p_rrn_dwh = v_rrn_dwh
    p_filemiladidate = v_filemiladidate

    LOG_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE = Logger(filename="__init__", level=__level__,
                                                       dirname="File-" + os.path.basename(
                                                           __file__) + "-Func-" + sys._getframe().f_code.co_name,
                                                       rootdir=__LOGDIR__)

    if __debugMODE__.upper() == 'TRUE':
        Important_single_log("Func_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE ",
                             "all income variable name "
                             + "\n v_diag: " + str(p_diag)
                             + "\n v_psp_bin: " + str(p_psp_bin)
                             + "\n v_terminal: " + str(p_terminal)
                             + "\n v_aut_date: " + str(p_aut_date)
                             + "\n v_amount: " + str(p_amount)
                             + "\n v_trace: " + str(p_trace)
                             + "\n v_bin_card: " + str(p_bin_card)
                             + "\n v_proccess_code: " + str(p_proccess_code)
                             + "\n v_device_type: " + str(p_device_type)
                             + "\n v_split_colmn: " + str(p_split_colmn)
                             + "\n v_status: " + str(p_status)
                             + "\n v_pkid: " + str(p_pkid)
                             + "\n v_filename: " + str(p_filename)
                             + "\n v_filedate: " + str(p_filedate)
                             + "\n v_insertdate: " + str(p_insertdate)
                             + "\n v_rrn_dwh: " + str(p_rrn_dwh)
                             + "\n v_filemiladidate: " + str(p_filemiladidate)
                             + "\n ------------------------"
                             )

    try:

        p_rescode = cursor780.var(int)
        p_res_msg = cursor780.var(str)
        cursor780.callproc('USSD.UNDECIDEDTRANS_INSERT_BEFORE',
                           (p_diag, p_psp_bin, p_terminal, p_aut_date, p_amount, p_trace,
                            p_bin_card, p_proccess_code, p_device_type, p_split_colmn, p_status,
                            p_pkid, p_filename, p_filedate, p_insertdate, p_rrn_dwh, p_filemiladidate,
                            p_rescode, p_res_msg))

        if __debugMODE__.upper() == 'TRUE':
            Important_single_log("Func_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE ",
                                 "all return variable name "
                                 + "\n v_return: " + str(p_rescode.getvalue())
                                 + "\n ------------------------"
                                 )

        return p_rescode.getvalue()

    except IndexError as ex:
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE.error("ERROR MASSAGE: " + str(ex))

    except cx_Oracle.DatabaseError as ex:
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE.warning("GENERAL ERROR DATABASE IN -> CALLFUNC_CHECK_TOPUP_STATUS()")
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE.error("ERROR MASSAGE: " + str(ex))
        return -1
    except Exception as ex:
        error, = ex.args
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE.error(str(error))
        return -1


# endregion Function CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE

# region Function CALLPROC_UNDECIDEDTRANS_INSERT_AFTER
def CALLPROC_UNDECIDEDTRANS_INSERT_AFTER(
        v_diag=None, v_psp_bin=None, v_terminal=None, v_aut_date=None, v_amount=None, v_trace=None, v_bin_card=None,
        v_proccess_code=None, v_device_type=None, v_split_colmn=None, v_status=None, v_pkid=None,
        v_filename=None, v_filedate=None, v_insertdate=None, v_rrn_dwh=None, v_filemiladidate=None,
        v_out_r_reserve1=None, v_out_r_reserve2=None
):
    p_diag = v_diag
    p_psp_bin = v_psp_bin
    p_terminal = v_terminal
    p_aut_date = v_aut_date
    p_amount = v_amount
    p_trace = v_trace
    p_bin_card = v_bin_card
    p_proccess_code = v_proccess_code
    p_device_type = v_device_type
    p_split_colmn = v_split_colmn
    p_status = v_status
    p_pkid = v_pkid
    p_filename = v_filename
    p_filedate = v_filedate
    p_insertdate = v_insertdate
    p_rrn_dwh = v_rrn_dwh
    p_filemiladidate = v_filemiladidate
    p_out_r_reserve1 = v_out_r_reserve1
    p_out_r_reserve2 = v_out_r_reserve2

    LOG_CALLPROC_UNDECIDEDTRANS_INSERT_AFTER = Logger(filename="__init__", level=__level__,
                                                      dirname="File-" + os.path.basename(
                                                          __file__) + "-Func-" + sys._getframe().f_code.co_name,
                                                      rootdir=__LOGDIR__)

    if __debugMODE__.upper() == 'TRUE':
        Important_single_log("Func_CALLPROC_UNDECIDEDTRANS_INSERT_AFTER ",
                             "all income variable name "
                             + "\n v_diag: " + str(p_diag)
                             + "\n v_psp_bin: " + str(p_psp_bin)
                             + "\n v_terminal: " + str(p_terminal)
                             + "\n v_aut_date: " + str(p_aut_date)
                             + "\n v_amount: " + str(p_amount)
                             + "\n v_trace: " + str(p_trace)
                             + "\n v_bin_card: " + str(p_bin_card)
                             + "\n v_proccess_code: " + str(p_proccess_code)
                             + "\n v_device_type: " + str(p_device_type)
                             + "\n v_split_colmn: " + str(p_split_colmn)
                             + "\n v_status: " + str(p_status)
                             + "\n v_pkid: " + str(p_pkid)
                             + "\n v_filename: " + str(p_filename)
                             + "\n v_filedate: " + str(p_filedate)
                             + "\n v_insertdate: " + str(p_insertdate)
                             + "\n v_rrn_dwh: " + str(p_rrn_dwh)
                             + "\n v_filemiladidate: " + str(p_filemiladidate)
                             + "\n v_out_r_reserve1: " + str(p_out_r_reserve1)
                             + "\n v_out_r_reserve2: " + str(p_out_r_reserve2)
                             + "\n ------------------------"
                             )

    try:

        p_rescode = cursor780.var(int)
        p_res_msg = cursor780.var(str)
        cursor780.callproc('USSD.UNDECIDEDTRANS_INSERT_AFTER',
                           (p_diag, p_psp_bin, p_terminal, p_aut_date, p_amount, p_trace,
                            p_bin_card, p_proccess_code, p_device_type, p_split_colmn, p_status,
                            p_pkid, p_filename, p_filedate, p_insertdate, p_rrn_dwh, p_filemiladidate,
                            p_out_r_reserve1, p_out_r_reserve2, p_rescode, p_res_msg))

        if __debugMODE__.upper() == 'TRUE':
            Important_single_log("Func_CALLPROC_UNDECIDEDTRANS_INSERT_AFTER ",
                                 "all return variable name "
                                 + "\n v_return: " + str(p_rescode.getvalue())
                                 + "\n ------------------------"
                                 )

        # logCallProc_UNDECIDEDTRANS_INSERT_AFTER.info(p_rescode.getvalue())
        # logCallProc_UNDECIDEDTRANS_INSERT_AFTER.info(p_res_msg.getvalue())

        return p_rescode.getvalue()

    except cx_Oracle.DatabaseError as ex:
        error, = ex.args
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_AFTER.error(str(error))
        return -1
    except Exception as ex:
        LOG_CALLPROC_UNDECIDEDTRANS_INSERT_AFTER.error(str(ex))
        return -1


# endregion Function CALLPROC_UNDECIDEDTRANS_INSERT_AFTER

# region Function LoadConfigFile
def loadConfigFile():
    LOG_LoadConfigFile = Logger(filename="__init__", level=__level__,
                                dirname="File-" + os.path.basename(
                                    __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:
        configINI = ConfigParser()
        configINI.read(__confiFileName__)

        global V_DB_USERNAME_780
        global V_DB_PASSWORD_780
        global V_DB_DSN_780

        global V_DB_USERNAME_PNA
        global V_DB_PASSWORD_PNA
        global V_DB_DSN_PNA

        global V_FTPS_SERVER
        global V_FTPS_PORT
        global V_FTPS_USER
        global V_FTPS_PASS
        global V_FTPS_RMT_DIR

        global V_MAX_FILEDATE

        global __debugMODE__

        __debugMODE__ = "FALSE"

        V_DB_USERNAME_780 = configINI.get('ORACLE_CONNECTION_780', 'dbUsername780')
        V_DB_PASSWORD_780 = configINI.get('ORACLE_CONNECTION_780', 'dbPassword780')
        V_DB_DSN_780 = configINI.get('ORACLE_CONNECTION_780', 'dbDSN780')

        V_DB_USERNAME_PNA = configINI.get('ORACLE_CONNECTION_PNA', 'dbUsernamePNA')
        V_DB_PASSWORD_PNA = configINI.get('ORACLE_CONNECTION_PNA', 'dbPasswordPNA')
        V_DB_DSN_PNA = configINI.get('ORACLE_CONNECTION_PNA', 'dbDSNPNA')

        V_FTPS_SERVER = configINI.get('FTPS_CONNECTION', 'ftpsServer')
        V_FTPS_PORT = configINI.get('FTPS_CONNECTION', 'ftpsPort')
        V_FTPS_USER = configINI.get('FTPS_CONNECTION', 'ftpsUser')
        V_FTPS_PASS = configINI.get('FTPS_CONNECTION', 'ftpsPass')
        V_FTPS_RMT_DIR = configINI.get('FTPS_CONNECTION', 'ftpsRmtDir')

        V_MAX_FILEDATE = configINI.get('GENERAL', 'v_max_filedate')
        __debugMODE__ = configINI.get('GENERAL', 'debug_mode')

        if __debugMODE__.upper() == 'TRUE':
            V_MAX_FILEDATE = 0
            Important_single_log("Func_loadConfigFile ",
                                 "all income variable name "
                                 + "\n V_DB_USERNAME_780: " + str(V_DB_USERNAME_780)
                                 + "\n V_DB_PASSWORD_780: " + str(V_DB_PASSWORD_780)
                                 + "\n V_DB_DSN_780: " + str(V_DB_DSN_780)
                                 + "\n V_DB_USERNAME_PNA: " + str(V_DB_USERNAME_PNA)
                                 + "\n V_DB_PASSWORD_PNA: " + str(V_DB_PASSWORD_PNA)
                                 + "\n V_DB_DSN_PNA: " + str(V_DB_DSN_PNA)
                                 + "\n V_FTPS_SERVER: " + str(V_FTPS_SERVER)
                                 + "\n V_FTPS_PORT: " + str(V_FTPS_PORT)
                                 + "\n V_FTPS_USER: " + str(V_FTPS_USER)
                                 + "\n V_FTPS_PASS: " + str(V_FTPS_PASS)
                                 + "\n V_FTPS_RMT_DIR: " + str(V_FTPS_RMT_DIR)
                                 + "\n V_MAX_FILEDATE: " + str(V_MAX_FILEDATE)
                                 + "\n ------------------------"
                                 )

    except:
        LOG_LoadConfigFile.error("UNEXPECTED ERROR FUNCTION loadConfigFile:" + str(sys.exc_info()[0]))


# endregion Function LoadConfigFile

# region Function InsertBefore
def insertBefore():
    logInsertBefore = Logger(filename="__init__", level=__level__,
                             dirname="File-" + os.path.basename(
                                 __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:

        for Vi_DIAG, Vi_PSP_BIN, Vi_TERMINAL, Vi_AUT_DATE, Vi_AMOUNT, Vi_TRACE, \
            Vi_BIN_CARD, Vi_PAN, Vi_PROCCESS_CODE, Vi_DEVICE_TYPE, Vi_SPLIT_COLMN, Vi_STATUS, Vi_PKID, \
            Vi_FILENAME, Vi_FILEDATE, Vi_INSERTDATE, Vi_RRN_DWH, Vi_FILEMILADIDATE in listforIterable:
            v_ret_before = CALLPROC_UNDECIDEDTRANS_INSERT_BEFORE(
                v_diag=Vi_DIAG, v_psp_bin=Vi_PSP_BIN, v_terminal=Vi_TERMINAL, v_aut_date=Vi_AUT_DATE,
                v_amount=Vi_AMOUNT,
                v_trace=Vi_TRACE, v_bin_card=Vi_BIN_CARD, v_proccess_code=Vi_PROCCESS_CODE,
                v_device_type=Vi_DEVICE_TYPE, v_split_colmn=Vi_SPLIT_COLMN, v_status=Vi_STATUS, v_pkid=Vi_PKID,
                v_filename=Vi_FILENAME, v_filedate=Vi_FILEDATE, v_insertdate=Vi_INSERTDATE, v_rrn_dwh=Vi_RRN_DWH,
                v_filemiladidate=Vi_FILEMILADIDATE
            )

            if __debugMODE__.upper() == 'TRUE':
                Important_single_log("Func_insertBefore ",
                                     "all return variable name "
                                     + "\n v_ret_before: " + str(v_ret_before)
                                     + "\n ------------------------"
                                     )

    except Exception as e:
        logInsertBefore.error("ERROR REGION FUNCTION_INSERTBEFORE: " + str(e))


# endregion Function InsertBefore

# region Function InsertAfter
def insertAfter():
    logInsertAfter = Logger(filename="__init__", level=__level__,
                            dirname="File-" + os.path.basename(
                                __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:
        global Vz_STATUS, out1, out2
        for Vi_DIAG, Vi_PSP_BIN, Vi_TERMINAL, Vi_AUT_DATE, Vi_AMOUNT, Vi_TRACE, \
            Vi_BIN_CARD, Vi_PAN, Vi_PROCCESS_CODE, Vi_DEVICE_TYPE, Vi_SPLIT_COLMN, Vi_STATUS, Vi_PKID, \
            Vi_FILENAME, Vi_FILEDATE, Vi_INSERTDATE, Vi_RRN_DWH, Vi_FILEMILADIDATE in listforIterable:
            if Vi_STATUS == "N":
                ret, out1, out2 = CALLFUNC_CHECK_TOPUP_STATUS(v_rrn=Vi_RRN_DWH, v_capdate=Vi_AUT_DATE)
                if ret == 0:
                    Vz_STATUS = "C"
                elif ret == 1:
                    Vz_STATUS = "P"
                else:
                    Vz_STATUS = "N"

            v_ret_after = CALLPROC_UNDECIDEDTRANS_INSERT_AFTER(
                v_diag=Vi_DIAG, v_psp_bin=Vi_PSP_BIN, v_terminal=Vi_TERMINAL, v_aut_date=Vi_AUT_DATE,
                v_amount=Vi_AMOUNT,
                v_trace=Vi_TRACE, v_bin_card=Vi_BIN_CARD, v_proccess_code=Vi_PROCCESS_CODE,
                v_device_type=Vi_DEVICE_TYPE, v_split_colmn=Vi_SPLIT_COLMN, v_status=Vz_STATUS, v_pkid=Vi_PKID,
                v_filename=Vi_FILENAME, v_filedate=Vi_FILEDATE, v_insertdate=Vi_INSERTDATE, v_rrn_dwh=Vi_RRN_DWH,
                v_filemiladidate=Vi_FILEMILADIDATE, v_out_r_reserve1=out1, v_out_r_reserve2=out2
            )

    except Exception as e:
        logInsertAfter.error("ERROR REGION FUNCTION_INSERTAFTER: " + str(e))


# endregion Function InsertAfter

# region Function Important_single_log
def Important_single_log(func_name, imp_err):
    log_important = Logger(filename="__init__", level=__level__,
                           dirname="File-" + os.path.basename(
                               __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    now = datetime.datetime.now()

    try:
        dirLOGIMP = __LOGDIR__ + '/' + now.strftime("%Y-%m-%d") + '/'
        if not os.path.exists(dirLOGIMP):
            os.makedirs(dirLOGIMP)

        with open(dirLOGIMP + "importantLOG-" + now.strftime("%Y-%m-%d") + ".log", 'a',
                  encoding='windows-1256') as writefile:
            writefile.write(func_name + " >> " + imp_err + "\n")

    except Exception as e:
        log_important.error(str(e))


# endregion Function Important_single_log

# region Function CompareTblAfter_AND_View
def compareTblAfter_AND_View():
    logCompareTblAfter_AND_View = Logger(filename="__init__", level=__level__,
                                         dirname="File-" + os.path.basename(
                                             __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    global V_REC_VIEW

    try:
        Vtoday_FILEMILADIDATE = Vn_FILEMILADIDATE[0]
        SQL_TBL_AFTER = """
                        SELECT COUNT(1) 
                        FROM USSD.ACQ_INQ_780_WITH_RRN_AFTER 
                        WHERE FILEMILADIDATE = :TEST
                        """

        SQL_VIEW = """
                    SELECT COUNT(1) 
                    FROM TEHRAN_INTERNET.ACQ_INQ_780_WITH_RRN 
                    """

        # RES_SQL_TBL_AFTER = cursor780.execFetchOne(SQL_TBL_AFTER, __LOGDIR__ + '/ORA')
        # cursor780.execArgs(SQL_TBL_AFTER, Vtoday_FILEMILADIDATE, __LOGDIR__ + '/ORA')
        cursor780.execute(SQL_TBL_AFTER, test=Vtoday_FILEMILADIDATE)
        RES_SQL_TBL_AFTER = cursor780.fetchall()

        # RES_SQL_VIEW = cursorPNA.execFetchOne(SQL_VIEW, __LOGDIR__ + '/ORA')
        RES_SQL_VIEW = cursorPNA.execute(SQL_VIEW).fetchone()

        regex_tbl_step1 = r"([(])"
        regex_tbl_step2 = r"([,)])"
        matches_tbl_step1 = re.sub(regex_tbl_step1, '\n', str(RES_SQL_TBL_AFTER[0]))
        matches_tbl_step2 = re.sub(regex_tbl_step2, '\n', matches_tbl_step1)
        V_REC_TBL = matches_tbl_step2.replace("\n", "")

        regex_view_step1 = r"([(])"
        regex_view_step2 = r"([,)])"
        matches_view_step1 = re.sub(regex_view_step1, '\n', str(RES_SQL_VIEW))
        matches_view_step2 = re.sub(regex_view_step2, '\n', matches_view_step1)
        V_REC_VIEW = matches_view_step2.replace("\n", "")

        Important_single_log("Func_CompareTableAfter_AND_VIEW >> COUNT TABLE AFTER ", V_REC_TBL)
        Important_single_log("Func_CompareTableAfter_AND_VIEW >> COUNT VIEW PNA ", V_REC_VIEW)

        if V_REC_TBL == V_REC_VIEW:
            # print("OK CompareTblAfter AND View")
            loadFromView()
            createFile(0)

        else:
            # print("table and view not countabddle matches")
            logCompareTblAfter_AND_View.error("TABLE AND VIEW NOT COUNTABLE MATCHES")
    except Exception as e:
        logCompareTblAfter_AND_View.error("ERROR REGION FUNCTION_COMPARETBLAFTER_AND_VIEW: " + str(e))


# endregion Function CompareTblAfter_AND_View

# region Function Create_Local_File
def createFile(cntV):
    logcreateFile = Logger(filename="__init__", level=__level__,
                           dirname="File-" + os.path.basename(
                               __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)

    cntV += 1
    # region check_is_file_pna_exist
    fileForFTP = str(Vn_FILENAME[0])
    if os.path.isfile(__ARC__ + fileForFTP):
        try:
            os.remove(__ARC__ + fileForFTP)
            createFile(1)
        except OSError as e:
            logcreateFile.error("ERROR: %s - %s." % (str(e.filename), str(e.strerror)))
    # endregion check_is_file_pna_exist

    # region create_file
    if not os.path.isfile(__ARC__ + fileForFTP):
        try:
            cntFile = 0
            # region open_and_fill_file
            for Vi_DIAG, Vi_PSP_BIN, Vi_TERMINAL, Vi_AUT_DATE, Vi_AMOUNT, Vi_TRACE, \
                Vi_BIN_CARD, Vi_PAN, Vi_PROCCESS_CODE, Vi_DEVICE_TYPE, Vi_SPLIT_COLMN, Vi_STATUS, Vi_PKID, \
                Vi_FILENAME, Vi_FILEDATE, Vi_INSERTDATE, Vi_RRN_DWH, Vi_FILEMILADIDATE in listforIterable:
                cntFile += 1
                with open(__ARC__ + str(Vn_FILENAME[0]), 'a',
                          encoding='windows-1256') as configfile:  # saveFile

                    # create file for bank with format shaparak.ir
                    if Vi_STATUS == "N":
                        ret1, ret2, ret3 = CALLFUNC_CHECK_TOPUP_STATUS(v_rrn=Vi_RRN_DWH, v_capdate=Vi_AUT_DATE)
                        if ret1 == 0:
                            Vz_STATUS = "C"
                        elif ret1 == 1:
                            Vz_STATUS = "P"
                        else:
                            Vz_STATUS = "N"

                        try:
                            global Vm_FILEMILADIDATE
                            Vm_FILEMILADIDATE = V_FTPS_RMT_DIR + "/" + Vi_FILEMILADIDATE

                            configfile.write(
                                Vi_DIAG + "|" + Vi_PSP_BIN + "|" + Vi_TERMINAL + "|" + Vi_AUT_DATE + "|" + Vi_AMOUNT
                                + "|" + Vi_TRACE + "|" + Vi_BIN_CARD + "|" + Vi_PAN + "|" + Vi_PROCCESS_CODE
                                + "|" + Vi_DEVICE_TYPE + "|" + Vi_SPLIT_COLMN + "|" + Vz_STATUS + "\n")

                            configfile.close()

                        except EnvironmentError as ex:
                            # parent of IOError, OSError *and* WindowsError where available
                            logcreateFile.warning(
                                "ERROR IN CREATE FILE " + __confiFileName__ + " -> REGION CREATE_FILE")
                            logcreateFile.error("ERROR MASSAGE: " + str(ex))

                        except:
                            logcreateFile.error(
                                "UNEXPECTED ERROR REGION CREATE_FILE #2:" + str(sys.exc_info()[0]))

                    else:
                        logcreateFile.error("NOT TAEEN VAZEYAT OR NOT -N- IN FILE")

                    configfile.close()

            # noinspection PySimplifyBooleanCheck
            if __debugMODE__.upper() == 'TRUE':
                Important_single_log("__DEBUG_MODE__", "Parameter debug is: " + str(__debugMODE__))
                Important_single_log("__DEBUG_MODE__",
                                     "---- count of record write to file is: " + str(cntFile) + "#FILE" + " ----")
                Important_single_log("__DEBUG_MODE__",
                                     "---- count of record write to file is: " + str(V_REC_VIEW) + "#VIEW" +
                                     str(cntV) + " ----")

            if str(cntFile) == str(V_REC_VIEW):
                # region send_ftp_pna
                if __SendFTP__.upper() == 'TRUE':

                    try:

                        ftp_conn = connect_ftp(ftpServer=V_FTPS_SERVER, ftpPort=V_FTPS_PORT,
                                               ftpUser=V_FTPS_USER, ftpPass=V_FTPS_PASS)

                        if ftp_conn:
                            Important_single_log("Func_Create_Local_File", Vm_FILEMILADIDATE)
                            ftps_upload_file(ftp_conn, fileForFTP, Vm_FILEMILADIDATE)
                        else:
                            logcreateFile.error("NO UPLOAD FILE TO REMOTE PATH: " + V_FTPS_RMT_DIR)
                    except Exception as e:
                        logcreateFile.error("ERROR: " + str(e))
                else:
                    logcreateFile.warning("FTP PARAMETER IS SET TO [FALSE]")
                # endregion send_ftp_pna

        except NameError as error:
            logcreateFile.error("ERROR MASSAGE: " + str(error))
        except EnvironmentError as ex:
            # parent of IOError, OSError *and* WindowsError where available
            logcreateFile.warning("ERROR IN CREATE" + __confiFileName__ + " -> createFile()")
            logcreateFile.error("ERROR MASSAGE: " + str(ex))
        except TypeError as e:
            logcreateFile.error("UNEXPECTED ERROR REGION CREATE_FILE TypeError: " + str(e))
        except:
            logcreateFile.error("UNEXPECTED ERROR REGION CREATE_FILE #1:" + str(sys.exc_info()[0]))
    # endregion create_file


# endregion Function Create_Local_File

# region Function FTPS_Check_directory_exists
def directory_exists(ftp_connection, r_dir):
    log_ftps_dir_exists = Logger(filename="__init__", level=__level__,
                                 dirname="File-" + os.path.basename(
                                     __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    # in current location
    filelist = []
    ftp_connection.retrlines('LIST', filelist.append)
    for f in filelist:
        if f.split()[-1] == r_dir and f.upper().startswith('D'):
            return True
        else:
            Important_single_log("Func_Directory_Exists ", "ERROR FUNCTION DIR_EXISTS: " + str(f.upper()))
            return True
    return False


# endregion Function FTPS_Check_directory_exists

# region Function FTPS_Change_directories
def ftps_chdir(ftp_connection, dir):
    # create if it doesn't exist
    if directory_exists(ftp_connection, dir) is False:  # (or negate, whatever you prefer for readability)
        ftp_connection.mkd(dir)
    else:
        Important_single_log("Func_FTPS_Chdir", str(dir) + " IS EXISTS")
    ftp_connection.cwd(dir)


# endregion Function FTPS_Change_directories

# region Function FTPS_UploadFile
def ftps_upload_file(ftp_connection, upload_file_path, rmt_dir):
    log_ftps_upload_file = Logger(filename="__init__", level=__level__,
                                  dirname="File-" + os.path.basename(
                                      __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)
    try:
        # ftp_connection.set_pasv(False)

        upload_file1 = open(os.path.join(__ARC__, upload_file_path), 'rb')
        # print('Uploading ' + str(upload_file1) + " ...")
        # ftp_connection.cwd(rmt_dir)
        ftps_chdir(ftp_connection, rmt_dir)
        ftp_connection.storbinary('STOR {}'.format(upload_file_path), upload_file1, 1000)
        # ftp_connection.storbinary('STOR ', upload_file1, 1000)
        ftp_connection.quit()
        ftp_connection.close()
        upload_file1.close()
        # print('Upload finished.')
    except Exception as e:
        log_ftps_upload_file.error("Error REGION FUNCTION_FTPS_UPLOAD_FILE: " + str(e))


# endregion Function FTPS_UploadFile

# region main_ut.py
if __name__ == '__main__':
    try:

        loadConfigFile()
        os.environ["PYTHONIOENCODING"] = "windows-1256"
        connection780 = None
        if __name__ == "__main__":
            logMain = Logger(filename="__init__", level=__level__,
                             dirname="File-" + os.path.basename(
                                 __file__) + "-Func-" + sys._getframe().f_code.co_name, rootdir=__LOGDIR__)

            try:
                # create instances of the dbHelper connection and cursor
                # connection780 = dbHelper.Connection(V_DB_USERNAME_780, V_DB_PASSWORD_780, V_DB_DSN_780,
                #                                     __LOGDIR__ + '/ORA')
                # cursor780 = connection780.cursor()
                connection780 = cx_Oracle.connect(V_DB_USERNAME_780, V_DB_PASSWORD_780, V_DB_DSN_780)
                cursor780 = connection780.cursor()

                # create instances of the dbHelper connection and cursor
                # connPNA = dbHelper.Connection("MAHDI", "Zz123456", "172.25.48.21:1521/ussd", __LOGDIR__ + '/ORA')
                # connPNA = dbHelper.Connection(V_DB_USERNAME_PNA, V_DB_PASSWORD_PNA, V_DB_DSN_PNA, __LOGDIR__ + '/ORA')
                # cursorPNA = connPNA.cursor()
                connPNA = cx_Oracle.connect(V_DB_USERNAME_PNA, V_DB_PASSWORD_PNA, V_DB_DSN_PNA)
                cursorPNA = connPNA.cursor()

                # demonstrate that the dbHelper connection and cursor are being used
                try:
                    # No commit as you don-t need to commit DDL.
                    V_NLS_LANGUAGE, = cursor780.execute("SELECT VALUE AS NLS_LANGUAGE "
                                                        "FROM V$NLS_PARAMETERS "
                                                        "WHERE PARAMETER = ('NLS_LANGUAGE')").fetchone()

                    V_NLS_TERRITORY, = cursor780.execute("SELECT VALUE AS NLS_TERRITORY "
                                                         "FROM V$NLS_PARAMETERS "
                                                         "WHERE PARAMETER = ('NLS_TERRITORY')").fetchone()

                    V_NLS_CHARACTERSET, = cursor780.execute("SELECT VALUE AS NLS_CHARACTERSET "
                                                            "FROM V$NLS_PARAMETERS WHERE "
                                                            "PARAMETER = ('NLS_CHARACTERSET')").fetchone()

                    if V_NLS_LANGUAGE and V_NLS_TERRITORY and V_NLS_CHARACTERSET is not None:
                        # export NLS_LANG=<language>_<territory>.<character set>
                        os.environ["NLS_LANG"] = V_NLS_LANGUAGE + "." + V_NLS_TERRITORY + "." + V_NLS_CHARACTERSET

                    try:
                        loadFromView()
                        insertBefore()

                        loadFromView()
                        insertAfter()

                        compareTblAfter_AND_View()

                    except Exception as ex:
                        logMain.error("ERROR IN CALL OTHER FUNCTION IN MAIN: " + str(ex))

                except Exception as e:
                    logMain.error(traceback.format_exc() + str(e))

                except:
                    logMain.error("UNEXPECTED ERROR REGION MAIN_UT.PY #3:" + str(sys.exc_info()[0]))

                # Ensure that we always disconnect from the database to avoid
                # ORA-00018: Maximum number of sessions exceeded.

            except cx_Oracle.DatabaseError as ex:
                logMain.warning("GENERAL ERROR DATABASE IN -> REGION MAIN_UT.PY")
                logMain.error("Error Massage: " + str(ex))
            except Exception as e:
                logMain.error(e)
                logMain.error("UNEXPECTED ERROR REGION MAIN_UT.PY #2:" + str(sys.exc_info()[0]))

    except RuntimeError as e:
        logMain.error(sys.stderr.write("ERROR: %s\n" % str(e)))

    except:
        logMain.error("UNEXPECTED ERROR REGION MAIN_UT.PY #1:" + str(sys.exc_info()[0]))
# endregion main_ut.py