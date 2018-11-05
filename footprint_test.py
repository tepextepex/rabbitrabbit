from auth import storage
from S1L1Tools import S1L1Tools
from S1L1Tools import get_footprint
from MonitoringUtils import *

test_product_name = "S1B_EW_GRDM_1SDH_20180718T023515_20180718T023551_011859_015D43_C280.zip"
work_dir = "/home/tepex/NIERSC/IEPI/monitoring/source/"

# s = S1L1Tools(work_dir + test_product_name)
# s.export_to_l2(work_dir)

# will create TIF footprint in geojson format:
test_product_name = "S1B_EW_GRDM_1SDH_20180718T023515_20180718T023551_011859_015D43_C280_HH.tif"
footprint_file_name = get_footprint(work_dir + test_product_name, work_dir)


