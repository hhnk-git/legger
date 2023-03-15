try:
    import pydevd as pydevd_pycharm
except ImportError:
    import sys
    sys.path.append('C:\\Program Files\\JetBrains\\PyCharm 2022.3.2\\debug-eggs\\pydevd-pycharm.egg')
    import pydevd_pycharm


try:
    pass

    pydevd_pycharm.settrace('localhost',
                            port=5555,
                            stdoutToServer=True,
                            stderrToServer=True,
                            suspend=False) #, trace_only_current_thread=True
except:
    pass
