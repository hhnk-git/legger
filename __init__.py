import logging
import os
import sys
from pathlib import Path

OUR_DIR = Path(__file__).parent

EXTERNAL_DEPENDENCY_DIR = OUR_DIR / "external"
THREEDI_DEPENDENCY_DIR = OUR_DIR.parent / "ThreeDiToolbox" / "deps"


log = logging.getLogger('legger')
log.setLevel(logging.DEBUG)

def _update_path(directories):
    """update path with directories."""
    for dir_path in directories:
        dir_path = Path(dir_path)
        if dir_path.exists():
            if str(dir_path) not in sys.path:
                sys.path.append(str(dir_path))
                log.info(f"{dir_path} added to sys.path")
        else:
            log.warning(
                f"{dir_path} does not exist and is not added to sys.path"
                )

_update_path([THREEDI_DEPENDENCY_DIR])

try:
    import pyqtgraph
except ImportError:
    log.info('no installation of pyqtgraph found, use one in external folder')
    _update_path([EXTERNAL_DEPENDENCY_DIR])

try:
    import sqlalchemy
except ImportError:
    log.info('no installation of sqlalchemy found, use one in external folder')
    _update_path([EXTERNAL_DEPENDENCY_DIR])

try:
    import geoalchemy2    
except ImportError:
    log.info('no installation of geoalchemy2 found, use one in external folder')
    _update_path([EXTERNAL_DEPENDENCY_DIR])


# try:
#     # temporary fix of libary geoalchemy2 in ThreeDiToolbox
#     geoalchemy_fix_file = os.path.join(tdi_external, 'geoalchemy2', '__init__.py')
#     f = open(geoalchemy_fix_file, 'r')
#     new_content = f.read().replace(
#         """
#                                 bind.execute("VACUUM %s" % table.name)""",
#         """
#                                 try:
#                                     bind.execute("VACUUM %s"%table.name)
#                                 except:
#                                     pass
#         """)
#     f.close()
#     f = open(geoalchemy_fix_file, 'w')
#     f.write(new_content)
#     f.close()
# except:
#     log.warning('patch geoalchemy does not work')
#
# sys.path.append(tdi_external)
import faulthandler

if sys.stderr is not None:
    pass
    # faulthandler.enable()

# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load main tool class
    :param iface: QgsInterface. A QGIS interface instance.
    """
    from .qgistools_plugin import Legger
    from legger.utils.qlogging import setup_logging

    setup_logging()
    return Legger(iface)
