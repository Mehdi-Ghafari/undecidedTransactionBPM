#!/usr/bin/env python
# -*- coding: windows-1256 -*-
import argparse
import re
import shutil
import traceback
import cx_Oracle
import logging
import sys
from configparser import ConfigParser

import strip as strip

from Logger import Logger
import os
from ftpsClass import connect_ftp
import datetime
import jdatetime

# region Function NewMaxENT
def NewMaxENT(V_MAX):
    config = ConfigParser()
    config.read(os.path.abspath('cfgDir\configFile.ini'))
    config['GENERAL']['v_max_ent'] = str(V_MAX)

    try:
        with open(__confiFileName__, 'w', encoding='windows-1256') as configfile:  # save
            config.write(configfile)

    except IndexError as ex:
        print("ERROR MASSAGE: " + str(ex))

    except EnvironmentError as ex:
        print("Error in create" + __confiFileName__ + " -> NewMaxID()")
        print("Error Massage: " + str(ex))
    except:
        print("Unexpected error:" + sys.exc_info()[0])

# endregion Function NewMaxENT

# region Function NewMaxEN
def NewMaxEN(V_MAX):
    config = ConfigParser()
    config.read(os.path.abspath('cfgDir\configFile.ini'))
    config['GENERAL']['v_max_en'] = str(V_MAX)

    try:
        with open(__confiFileName__, 'w', encoding='windows-1256') as configfile:  # save
            config.write(configfile)

    except IndexError as ex:
        print("ERROR MASSAGE: " + str(ex))

    except EnvironmentError as ex:
        print("Error in create" + __confiFileName__ + " -> NewMaxID()")
        print("Error Massage: " + str(ex))
    except:
        print("Unexpected error:" + sys.exc_info()[0])

# endregion Function NewMaxEN

# region Function MergeFile
def mergeFile(pathEN, pathENT, pathOUT):
    with open(pathEN) as file1:
        txt1 = file1.read().splitlines()

    with open(pathENT) as file2:
        txt2 = file2.read().splitlines()

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


def createFile(vfnOUT1, folder):
    # open the file for reading
    filehandle = open(os.path.abspath(vfnOUT1), 'r')
    while True:
        # read a single line
        line = filehandle.readline()
        if not line:
            break
        vRRN = line.split('|')[0]
        vDIAG = line.split('|')[1]
        vPSP_BIN = line.split('|')[2]
        vCAPTUREDATE = line.split('|')[3]
        vCAPTUREDATE2 = vCAPTUREDATE[0:8]
        vAMOUNT = line.split('|')[4]
        vTRACE = line.split('|')[5]
        vBANKID = line.split('|')[6]
        vCARD = line.split('|')[7]
        vPROCCESS_CODE = line.split('|')[8]
        vDEVICE_TYPE = line.split('|')[9]
        vSPLIT_COLMN = line.split('|')[10]
        vSTATUS = line.split('|')[11]

        with open('OUTMA.txt', 'a',
                  encoding='windows-1256') as configfile:  # saveFile

            # create file for bank with format shaparak.ir
            if vSTATUS.strip() == "N":
                ret1, ret2, ret3 = CALLFUNC_CHECK_TOPUP_STATUS(v_rrn=vRRN, v_capdate=vCAPTUREDATE2)
                if ret1 == 0:
                    Vz_STATUS = "C"
                elif ret1 == 1:
                    Vz_STATUS = "P"
                else:
                    Vz_STATUS = "N"

                try:
                    # global Vm_FILEMILADIDATE
                    # Vm_FILEMILADIDATE = V_FTPS_RMT_DIR + "/" + Vi_FILEMILADIDATE

                    configfile.write(
                        vDIAG + "|" + vPSP_BIN + "|" + vCAPTUREDATE + "|" + vAMOUNT + "|" + vTRACE
                        + "|" + vBANKID + "|" + vCARD + "|" + vPROCCESS_CODE + "|" + vDEVICE_TYPE
                        + "|" + vSPLIT_COLMN + "|" + Vz_STATUS + "\n")


                    print(vDIAG + "|" + vPSP_BIN + "|" + vCAPTUREDATE + "|" + vAMOUNT)

                    configfile.close()



                except EnvironmentError as ex:
                    # print("ERROR IN CREATE FILE " + __confiFileName__ + " -> REGION CREATE_FILE")
                    print("ERROR MASSAGE: " + str(ex))
                except:
                    print("UNEXPECTED ERROR REGION CREATE_FILE #2:" + str(sys.exc_info()[0]))

            else:
                print("NOT TAEEN VAZEYAT OR NOT -N- IN FILE")

            configfile.close()


    # close the pointer to that file
    filehandle.close()

    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

# endregion Function MergeFile

# region Function CALLFUNC_CHECK_TOPUP_STATUS
def CALLFUNC_CHECK_TOPUP_STATUS(v_msisdn=None, v_trace=None, v_desc=None, v_adddata=None, v_amount=0, v_rrn=None,
                                v_capdate=None):
    P_MSISDN = v_msisdn
    P_TRACE = v_trace
    P_DESC = v_desc
    P_ADDDATA = v_adddata
    P_AMOUNT = v_amount
    P_RESERVE1 = v_rrn
    P_RESERVE2 = v_capdate
    R_RESERVE1 = cursor780.var(str)
    R_RESERVE2 = cursor780.var(str)

    try:
        V_RET = cursor780.callfunc('USSD.CHECK_TOPUP_STATUS2', int, [P_MSISDN, P_TRACE, P_DESC, P_ADDDATA, P_AMOUNT,
                                                                     P_RESERVE1, P_RESERVE2, R_RESERVE1, R_RESERVE2])

        return V_RET, R_RESERVE1.getvalue(), R_RESERVE2.getvalue()

    except IndexError as ex:
        print("ERROR MASSAGE: " + str(ex))

    except cx_Oracle.DatabaseError as ex:
        print("GENERAL ERROR DATABASE IN -> CALLFUNC_CHECK_TOPUP_STATUS()")
        print("ERROR MASSAGE: " + str(ex))
    except:
        print("UNEXPECTED ERROR FUNCTION -> CALLFUNC_CHECK_TOPUP_STATUS:" + str(sys.exc_info()[0]))
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

    try:

        p_rescode = cursor780.var(int)
        p_res_msg = cursor780.var(str)
        cursor780.callproc('USSD.UNDECIDEDTRANS_INSERT_BEFORE',
                           (p_diag, p_psp_bin, p_terminal, p_aut_date, p_amount, p_trace,
                            p_bin_card, p_proccess_code, p_device_type, p_split_colmn, p_status,
                            p_pkid, p_filename, p_filedate, p_insertdate, p_rrn_dwh, p_filemiladidate,
                            p_rescode, p_res_msg))

        return p_rescode.getvalue()

    except IndexError as ex:
        print("ERROR MASSAGE: " + str(ex))

    except cx_Oracle.DatabaseError as ex:
        print("GENERAL ERROR DATABASE IN -> CALLFUNC_CHECK_TOPUP_STATUS()")
        print("ERROR MASSAGE: " + str(ex))
        return -1
    except Exception as ex:
        error, = ex.args
        print(str(error))
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

    try:

        p_rescode = cursor780.var(int)
        p_res_msg = cursor780.var(str)
        cursor780.callproc('USSD.UNDECIDEDTRANS_INSERT_AFTER',
                           (p_diag, p_psp_bin, p_terminal, p_aut_date, p_amount, p_trace,
                            p_bin_card, p_proccess_code, p_device_type, p_split_colmn, p_status,
                            p_pkid, p_filename, p_filedate, p_insertdate, p_rrn_dwh, p_filemiladidate,
                            p_out_r_reserve1, p_out_r_reserve2, p_rescode, p_res_msg))

        # logCallProc_UNDECIDEDTRANS_INSERT_AFTER.info(p_rescode.getvalue())
        # logCallProc_UNDECIDEDTRANS_INSERT_AFTER.info(p_res_msg.getvalue())

        return p_rescode.getvalue()

    except cx_Oracle.DatabaseError as ex:
        error, = ex.args
        print(str(error))
        return -1
    except Exception as ex:
        print(str(ex))
        return -1


# endregion Function CALLPROC_UNDECIDEDTRANS_INSERT_AFTER

# region Function LoadConfigFile
def loadConfigFile():
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


    except:
        print("UNEXPECTED ERROR FUNCTION loadConfigFile:" + str(sys.exc_info()[0]))


# endregion Function LoadConfigFile

# region Function loadOUTFile
def createFileOUT():
    vfnEN = '780\EN\\' + lastFile('780\EN')
    vfnENTRAN = '780\EN TARAKONESH\\' + lastFile('780\EN TARAKONESH')

    NewMaxEN(vfnEN)
    NewMaxENT(vfnENTRAN)

    folder = os.path.abspath('temp\\')

    if len(os.listdir(folder)) == 0:
        vfnOUT1 = 'temp\\' + lastFile('780\EN')
        try:
            mergeFile(vfnEN, vfnENTRAN, vfnOUT1)
            createFile(vfnOUT1, folder)
        except Exception as e:
            print("error in def createFileOUT: " + e)
    else:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))
        createFileOUT()

# endregion Function loadOUTFile

# region Function FTPS_Check_directory_exists
def directory_exists(ftp_connection, r_dir):
    # in current location
    filelist = []
    ftp_connection.retrlines('LIST', filelist.append)
    for f in filelist:
        if f.split()[-1] == r_dir and f.upper().startswith('D'):
            return True
        else:
            print("Func_Directory_Exists ", "ERROR FUNCTION DIR_EXISTS: " + str(f.upper()))
            return True
    return False


# endregion Function FTPS_Check_directory_exists

# region Function FTPS_Change_directories
def ftps_chdir(ftp_connection, dir):
    # create if it doesn't exist
    if directory_exists(ftp_connection, dir) is False:  # (or negate, whatever you prefer for readability)
        ftp_connection.mkd(dir)
    else:
        print("Func_FTPS_Chdir", str(dir) + " IS EXISTS")
    ftp_connection.cwd(dir)


# endregion Function FTPS_Change_directories

# region Function FTPS_UploadFile
def ftps_upload_file(ftp_connection, upload_file_path, rmt_dir):
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
        print("Error REGION FUNCTION_FTPS_UPLOAD_FILE: " + str(e))


# endregion Function FTPS_UploadFil

# region last_file
def get_date(filename):
    date_pattern = re.compile(r'\b(\d{4})(\d{2})(\d{2})\b')
    matched = date_pattern.search(filename)

    if not matched:
        return None
    m, d, y = map(int, matched.groups())
    return jdatetime.date(m, d, y).togregorian()


def lastFile(fn):
    # arrFilename = os.listdir('780\EN TARAKONESH')
    arrFilename = os.listdir(fn)
    dates = (get_date(fn) for fn in arrFilename)
    dates = (d for d in dates if d is not None)
    last_date = max(dates)
    # last_date = last_date.strftime('%Y-%m-%d')
    last_j = jdatetime.datetime.fromgregorian(day=int(last_date.strftime('%d')), month=int(last_date.strftime('%m')),
                                              year=int(last_date.strftime('%Y'))).strftime("%Y%m%d")
    filenames = [fn for fn in arrFilename if last_j in fn]
    for fn in filenames:
        return fn


# endregion last_file

# region main_ut.py
if __name__ == '__main__':
    __ARC__ = os.path.abspath(str('780\ArchDir'))
    try:
        __confiFileName__ = os.path.abspath('cfgDir/configFile.ini')
        __SendFTP__ = "TRUE"
    except:
        print("Noting variable set")

    try:
        # loadConfigFile()
        os.environ["PYTHONIOENCODING"] = "windows-1256"
        connection780 = None
        if __name__ == "__main__":
            try:
                configINI = ConfigParser()
                configINI.read(os.path.abspath('cfgDir\configFile.ini'))

                V_DB_USERNAME_780 = configINI.get('ORACLE_CONNECTION_780', 'dbUsername780')
                V_DB_PASSWORD_780 = configINI.get('ORACLE_CONNECTION_780', 'dbPassword780')
                V_DB_DSN_780 = configINI.get('ORACLE_CONNECTION_780', 'dbDSN780')

                connection780 = cx_Oracle.connect(V_DB_USERNAME_780, V_DB_PASSWORD_780, V_DB_DSN_780)
                cursor780 = connection780.cursor()
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
                        #createFileOUT()
                        # loadFromView()
                        # insertBefore()
                        # loadFromView()
                        # insertAfter()
                        loadConfigFile()

                        ftp_conn = connect_ftp(ftpServer=V_FTPS_SERVER, ftpPort=V_FTPS_PORT,
                                               ftpUser=V_FTPS_USER, ftpPass=V_FTPS_PASS)

                    except Exception as ex:
                        print("ERROR IN CALL OTHER FUNCTION IN MAIN: " + str(ex))

                except Exception as e:
                    print(traceback.format_exc() + str(e))

                except:
                    print("UNEXPECTED ERROR REGION MAIN_UT.PY #3:" + str(sys.exc_info()[0]))

                # Ensure that we always disconnect from the database to avoid
                # ORA-00018: Maximum number of sessions exceeded.

            except cx_Oracle.DatabaseError as ex:
                print("GENERAL ERROR DATABASE IN -> REGION MAIN_UT.PY")
                print("Error Massage: " + str(ex))
            except Exception as e:
                print(e)
                print("UNEXPECTED ERROR REGION MAIN_UT.PY #2:" + str(sys.exc_info()[0]))

    except RuntimeError as e:
        print(sys.stderr.write("ERROR: %s\n" % str(e)))

    except:
        print("UNEXPECTED ERROR REGION MAIN_UT.PY #1:" + str(sys.exc_info()[0]))

# endregion main_ut.py