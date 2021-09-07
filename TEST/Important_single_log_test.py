# region Function Important_single_log
def Important_single_log(func_name, imp_err):
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