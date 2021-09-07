# region try_main
import cx_Oracle

try:


    os.environ["PYTHONIOENCODING"] = "windows-1256"
    connection780 = None
    if __name__ == "__main__":
        try:
            connection780 = cx_Oracle.connect(V_DB_USERNAME_780, V_DB_PASSWORD_780, V_DB_DSN_780)
            cursor780 = connection780.cursor()

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

# endregion try_main