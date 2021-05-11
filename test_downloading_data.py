from download_price_data import loop_every_500_rows_and_make_request
import requests
import pandas as pd
import finnhub
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import time
import os


def test():
    loop_every_500_rows_and_make_request(datetime(2010, 4, 20), datetime(2020, 4, 23))
test()