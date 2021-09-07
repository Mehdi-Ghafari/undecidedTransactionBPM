# region Function Create_Local_File
def createFile(cntV):
    cntV += 1
    # region check_is_file_pna_exist
    fileForFTP = str(Vn_FILENAME[0])
    if os.path.isfile(__ARC__ + fileForFTP):
        try:
            os.remove(__ARC__ + fileForFTP)
            createFile(1)
        except OSError as e:
            print("ERROR: %s - %s." % (str(e.filename), str(e.strerror)))
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
                            print("ERROR IN CREATE FILE " + __confiFileName__ + " -> REGION CREATE_FILE")
                            print("ERROR MASSAGE: " + str(ex))
                        except:
                            print("UNEXPECTED ERROR REGION CREATE_FILE #2:" + str(sys.exc_info()[0]))

                    else:
                        print("NOT TAEEN VAZEYAT OR NOT -N- IN FILE")

                    configfile.close()

            if str(cntFile) == str(V_REC_VIEW):
                # region send_ftp_pna
                if __SendFTP__.upper() == 'TRUE':

                    try:

                        ftp_conn = connect_ftp(ftpServer=V_FTPS_SERVER, ftpPort=V_FTPS_PORT,
                                               ftpUser=V_FTPS_USER, ftpPass=V_FTPS_PASS)

                        if ftp_conn:
                            print("Func_Create_Local_File", Vm_FILEMILADIDATE)
                            ftps_upload_file(ftp_conn, fileForFTP, Vm_FILEMILADIDATE)
                        else:
                            print("NO UPLOAD FILE TO REMOTE PATH: " + V_FTPS_RMT_DIR)
                    except Exception as e:
                        print("ERROR: " + str(e))
                else:
                    print("FTP PARAMETER IS SET TO [FALSE]")
                # endregion send_ftp_pna

        except NameError as error:
            print("ERROR MASSAGE: " + str(error))
        except EnvironmentError as ex:
            # parent of IOError, OSError *and* WindowsError where available
            print("ERROR IN CREATE" + __confiFileName__ + " -> createFile()")
            print("ERROR MASSAGE: " + str(ex))
        except TypeError as e:
            print("UNEXPECTED ERROR REGION CREATE_FILE TypeError: " + str(e))
        except:
            print("UNEXPECTED ERROR REGION CREATE_FILE #1:" + str(sys.exc_info()[0]))
    # endregion create_file


# endregion Function Create_Local_File
